// src/llm/openai-client.js
// OpenAI 兼容 LLM 流式客户端：原生 fetch + SSE 零依赖（Node 18+ 自带）。
// 关键约束（DeepSeek 官方文档 2026-08）：端点 POST {baseUrl}/chat/completions；
// thinking 默认 enabled，必须显式传 {"type":"disabled"} 关闭；流式增量走 choices[0].delta。
// 三配置（baseUrl/apiKey/model）全部参数注入，不硬编码任何密钥；无全局状态，实例可并发多流。

/**
 * @typedef {Object} OpenAiClientConfig
 * @property {string} baseUrl - OpenAI 兼容端点根地址（如 https://api.deepseek.com，不带 /chat/completions）
 * @property {string} apiKey - API key（由环境变量/配置注入，禁止硬编码）
 * @property {string} model - 模型名（如 deepseek-v4-flash）
 */

/**
 * @typedef {Object} ChatChunk
 * @property {string} content - 正文增量
 * @property {string} [reasoning_content] - 思考增量（thinking 关闭时通常无）
 * @property {Array<{id: string, name: string, arguments: string}>} [toolCalls] - 流结束时的工具调用（完整累积）
 */

/**
 * @typedef {Object} StreamChatOptions
 * @property {Array<{type: string, name: string, description?: string, parameters?: object}>} [tools]
 *   - OpenAI 兼容 function tools 描述（agent-consult 场景由 gateway 注入 openclaw_agent_consult/control）
 */

/** 校验工厂入参（参考 sherpa-stt.js createStt 的边界校验风格）。 */
function validateConfig(config) {
  if (!config || typeof config !== 'object') {
    throw new TypeError('createOpenAiClient: config object is required');
  }
  const { baseUrl, apiKey, model } = config;
  if (typeof baseUrl !== 'string' || baseUrl.length === 0) {
    throw new TypeError('createOpenAiClient: baseUrl is required (OpenAI-compatible endpoint root)');
  }
  if (typeof apiKey !== 'string' || apiKey.length === 0) {
    throw new TypeError('createOpenAiClient: apiKey is required (inject from env/config, never hardcode)');
  }
  if (typeof model !== 'string' || model.length === 0) {
    throw new TypeError('createOpenAiClient: model is required');
  }
}

/** 拼接 chat/completions 端点：去尾部斜杠后追加路径，兼容带/不带 /v1 前缀的 baseUrl。 */
function buildChatUrl(baseUrl) {
  const normalized = baseUrl.replace(/\/+$/, '');
  return `${normalized}/chat/completions`;
}

/** 组装请求头：Bearer 鉴权 + JSON 内容类型。 */
function buildHeaders(apiKey) {
  return {
    'Content-Type': 'application/json',
    Authorization: `Bearer ${apiKey}`,
  };
}

/** 组装请求体：stream 开启 + thinking 显式 disabled（DeepSeek 默认 enabled，必须显式关）；
 *  tools 可选：OpenAI 兼容 function tools（agent-consult 场景注入 consult/control 工具）。 */
function buildRequestBody(model, messages, tools) {
  const body = {
    model,
    messages,
    stream: true,
    thinking: { type: 'disabled' },
  };
  if (Array.isArray(tools) && tools.length > 0) {
    body.tools = tools;
  }
  return JSON.stringify(body);
}

/** 非 2xx → 构造 Error：status + body 摘要（截断 300 字符，避免泄露敏感内容）。 */
async function buildHttpError(res) {
  let detail = '';
  try {
    const text = await res.text();
    detail = text.slice(0, 300);
  } catch {
    // body 读取失败不影响主错误
  }
  return new Error(`OpenAI client: HTTP ${res.status}${detail ? `: ${detail}` : ''}`);
}

/**
 * 解析单行 SSE，返回：
 * - { done: true } —— [DONE] 终止标记
 * - { content?, reasoning_content?, toolCallDelta? } —— 含增量的数据行（至少一个字段非空）
 * - null —— 跳过（非 data: 行 / 空载荷 / JSON.parse 失败，不中断流）
 * toolCallDelta: delta.tool_calls 原始数组（OpenAI 兼容 function calling 流式增量，
 * 每个元素 {index, id?, function?: {name?, arguments?}}；由 streamChat 按 index 累积）。
 * @param {string} line
 * @returns {{done: true} | ChatChunk | null}
 */
function parseSseLine(line) {
  if (!line.startsWith('data:')) return null;
  const data = line.slice(5).trim();
  if (data === '') return null;
  if (data === '[DONE]') return { done: true };
  let payload;
  try {
    payload = JSON.parse(data);
  } catch {
    return null; // 解析失败跳过并继续，不因单行坏数据终止流
  }
  const delta = payload.choices?.[0]?.delta ?? {};
  const chunk = {};
  if (typeof delta.content === 'string' && delta.content !== '') {
    chunk.content = delta.content;
  }
  if (typeof delta.reasoning_content === 'string' && delta.reasoning_content !== '') {
    chunk.reasoning_content = delta.reasoning_content;
  }
  if (Array.isArray(delta.tool_calls) && delta.tool_calls.length > 0) {
    chunk.toolCallDelta = delta.tool_calls;
  }
  return Object.keys(chunk).length > 0 ? chunk : null;
}

/** 累积流式 tool_call 增量：按 index 合并 id/name/arguments（arguments 为 JSON 字符串片段拼接）。 */
function accumulateToolCall(acc, delta) {
  const index = typeof delta.index === 'number' ? delta.index : 0;
  const entry = acc[index] ?? { id: '', name: '', arguments: '' };
  if (delta.id) entry.id = delta.id;
  if (delta.function?.name) entry.name = delta.function.name;
  if (typeof delta.function?.arguments === 'string') entry.arguments += delta.function.arguments;
  acc[index] = entry;
  return acc;
}

/**
 * 创建 OpenAI 兼容 LLM 客户端。
 * @param {OpenAiClientConfig} config
 * @returns {{streamChat: (messages: Array<{role: string, content: string}>) => AsyncGenerator<ChatChunk>}}
 */
export function createOpenAiClient(config) {
  validateConfig(config);
  const { baseUrl, apiKey, model } = config;

  return {
    /**
     * 流式 chat completion：逐 chunk 产出增量文本。
     * 网络失败/非 2xx 在迭代时抛错；实例独立无共享状态，可并发多个流。
     * @param {Array<{role: string, content: string}>} messages
     * @param {StreamChatOptions} [options] - 可选 { tools }（OpenAI 兼容 function tools）
     */
    streamChat: async function* (messages, options = {}) {
      if (!Array.isArray(messages) || messages.length === 0) {
        throw new TypeError('streamChat: messages must be a non-empty array');
      }
      const tools = Array.isArray(options.tools) ? options.tools : undefined;
      const res = await fetch(buildChatUrl(baseUrl), {
        method: 'POST',
        headers: buildHeaders(apiKey),
        body: buildRequestBody(model, messages, tools),
      });
      if (!res.ok) {
        throw await buildHttpError(res);
      }
      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      /** @type {Record<number, {id: string, name: string, arguments: string}>} tool_call 按 index 累积 */
      const toolAccum = {};
      let toolCallsYielded = false;
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        // SSE 按行切分：最后一段可能不完整，留到下一个 chunk 拼接
        const lines = buffer.split('\n');
        buffer = lines.pop() ?? '';
        for (const line of lines) {
          const chunk = parseSseLine(line);
          if (chunk === null) continue;
          if (chunk.done) {
            // 流结束：若累积了 tool_calls 且尚未产出，以 { toolCalls } 形式产出一次
            const toolCalls = Object.values(toolAccum)
              .filter((tc) => tc.id && tc.name)
              .map((tc) => ({ id: tc.id, name: tc.name, arguments: tc.arguments }));
            if (toolCalls.length > 0 && !toolCallsYielded) {
              toolCallsYielded = true;
              yield { toolCalls };
            }
            return;
          }
          if (chunk.toolCallDelta) {
            for (const delta of chunk.toolCallDelta) {
              accumulateToolCall(toolAccum, delta);
            }
          }
          if (chunk.content || chunk.reasoning_content) {
            yield chunk;
          }
        }
      }
    },
  };
}
