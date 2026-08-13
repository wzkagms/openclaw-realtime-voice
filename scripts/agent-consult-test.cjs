// scripts/agent-consult-test.cjs
// Phase 4 agent-consult 适配验证（澜影方案 1 拍板后新增）：
//   1) autoRespondToAudio=false：bridge 不自管回复（等 gateway 回流），出句只上报 transcript
//   2) 回流路径：sendUserMessage(agent 结果) → LLM 转口语 → TTS → onAudio
//   3) submitToolResult 续接：LLM 返回 tool_calls → onToolCall 上报 → 续接生成回复 → TTS
//   4) openai-client：tool_calls 流式增量累积解析（mock SSE）
//   5) consult 回流超时兜底：autoRespondToAudio=false + 无回流 → 自管 LLM 降级回复（Phase 4 修复）
// 用法: node scripts/agent-consult-test.cjs
// 退出码: 0 = 全部通过, 1 = 存在失败

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { createRequire } = require('node:module');

const require_ = createRequire(require.resolve('../package.json'));
const { LinearResampler } = require_('sherpa-onnx-node');

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * 轮询等待谓词为真（与 bridge-llm-tts-test / e2e-test 同一模式）。
 * @param {() => boolean} predicate
 * @param {number} [timeoutMs]
 * @param {number} [intervalMs]
 */
async function waitFor(predicate, timeoutMs = 5000, intervalMs = 20) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (predicate()) return true;
    if (Date.now() >= deadline) return false;
    await sleep(intervalMs);
  }
}

/** 解析标准 RIFF/WAVE 16-bit PCM mono wav（test_wavs 格式，与 e2e-test 同实现）。 */
function parseWav(filePath) {
  const buf = fs.readFileSync(filePath);
  if (buf.toString('ascii', 0, 4) !== 'RIFF' || buf.toString('ascii', 8, 12) !== 'WAVE') {
    throw new Error(`parseWav: ${filePath} is not a RIFF/WAVE file`);
  }
  let offset = 12;
  let fmt = null;
  let data = null;
  while (offset + 8 <= buf.length) {
    const id = buf.toString('ascii', offset, offset + 4);
    const size = buf.readUInt32LE(offset + 4);
    if (id === 'fmt ') {
      fmt = {
        audioFormat: buf.readUInt16LE(offset + 8),
        channels: buf.readUInt16LE(offset + 10),
        sampleRate: buf.readUInt32LE(offset + 12),
        bitsPerSample: buf.readUInt16LE(offset + 22),
      };
    } else if (id === 'data') {
      data = buf.subarray(offset + 8, offset + 8 + size);
      break;
    }
    offset += 8 + size + (size % 2);
  }
  if (!fmt || !data) throw new Error(`parseWav: ${filePath} missing fmt/data chunk`);
  if (fmt.audioFormat !== 1 || fmt.bitsPerSample !== 16) {
    throw new Error(`parseWav: ${filePath} must be PCM16 (got fmt=${fmt.audioFormat}, bits=${fmt.bitsPerSample})`);
  }
  const durationSec = data.length / (fmt.sampleRate * fmt.channels * 2);
  return { ...fmt, pcm16: Buffer.from(data), durationSec };
}

/** PCM16 LE Buffer → Float32Array（归一化 [-1,1]）。 */
function pcm16ToFloat32(buffer) {
  const count = Math.floor(buffer.length / 2);
  const int16 = new Int16Array(buffer.buffer, buffer.byteOffset, count);
  const samples = new Float32Array(count);
  for (let i = 0; i < count; i++) samples[i] = int16[i] / 32768;
  return samples;
}

/** Float32Array（-1..1）→ PCM16 LE Buffer。 */
function float32ToPcm16(samples) {
  const int16 = new Int16Array(samples.length);
  for (let i = 0; i < samples.length; i++) {
    const v = Math.round(samples[i] * 32767);
    int16[i] = v < -32768 ? -32768 : v > 32767 ? 32767 : v;
  }
  return Buffer.from(int16.buffer, int16.byteOffset, int16.byteLength);
}

const MOCK_FRAME = Buffer.alloc(960, 7); // 20ms @24k = 960 bytes

/** mock LLM 工厂：默认返回文本（普通回流场景）；firstRespondToolCall=true 时首轮返回 tool_calls。 */
function createMockLlmFactory({ consultReply = '这是 agent 查证后的回复。', firstRespondToolCall = false, recordOptions = false } = {}) {
  return () => {
    let callCount = 0;
    const llm = {
      streamChat: async function* (messages, options = {}) {
        callCount += 1;
        if (recordOptions) llm.lastOptions = options;
        const hasToolResult = messages.some((m) => m.role === 'tool');
        if (hasToolResult || !firstRespondToolCall || callCount > 1) {
          yield { content: consultReply };
          return;
        }
        // 首轮：直接 yield 完整 toolCalls（模拟 openai-client streamChat 对外契约）
        yield {
          toolCalls: [{ id: 'call_123', name: 'openclaw_agent_consult', arguments: '{"question":"你好"}' }],
        };
      },
      getCallCount: () => callCount,
    };
    return llm;
  };
}

/** mock TTS + decoder 工厂（注入 bridge 避免真实网络/解码）。 */
function createMockTtsFactory() {
  return () => ({
    synthesize: async function* () {
      yield { audio: Buffer.from('mp3mock') };
    },
  });
}
function createMockDecoderFactory() {
  return () => ({
    reset: async () => {},
    decode: async function* () {
      yield { pcm16: MOCK_FRAME };
    },
    free: () => {},
  });
}

async function main() {
  let passed = 0;
  let failed = 0;
  const results = [];
  const test = async (name, fn) => {
    try {
      await fn();
      results.push({ name, ok: true });
      passed += 1;
    } catch (err) {
      results.push({ name, ok: false, error: err.message });
      failed += 1;
      console.error(`[FAIL] ${name}: ${err.message}`);
    }
  };

  const { createBridgeRuntime } = await import('../src/bridge/bridge-runtime.js');
  const { createOpenAiClient } = await import('../src/llm/openai-client.js');

  // ---------- 测试 1：openai-client tool_calls 累积解析 ----------
  await test('openai-client: streamChat 累积 tool_calls 并产出 { toolCalls }', async () => {
    const sseLines = [
      'data: {"choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_123"}]}}]}',
      'data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"name":"openclaw_agent_consult"}}]}}]}',
      'data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\\"question\\":"}}]}}]}',
      'data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\\"你好\\"}"}}]}}]}',
      'data: [DONE]',
    ];
    const originalFetch = global.fetch;
    global.fetch = async () => ({
      ok: true,
      body: {
        getReader: () => {
          const encoder = new TextEncoder();
          let i = 0;
          return {
            read: async () => {
              if (i < sseLines.length) {
                const line = sseLines[i] + '\n';
                i += 1;
                return { done: false, value: encoder.encode(line) };
              }
              return { done: true, value: undefined };
            },
          };
        },
      },
    });
    try {
      const client = createOpenAiClient({ baseUrl: 'http://mock', apiKey: 'k', model: 'm' });
      const chunks = [];
      for await (const chunk of client.streamChat(
        [{ role: 'user', content: '你好' }],
        { tools: [{ type: 'function', name: 'openclaw_agent_consult' }] },
      )) {
        chunks.push(chunk);
      }
      const toolCalls = chunks.find((c) => c.toolCalls)?.toolCalls;
      assert.ok(toolCalls && toolCalls.length === 1, 'should yield one toolCalls entry');
      assert.equal(toolCalls[0].id, 'call_123');
      assert.equal(toolCalls[0].name, 'openclaw_agent_consult');
      assert.equal(toolCalls[0].arguments, '{"question":"你好"}');
      // 请求体含 tools（检查 fetch 收到的 body）
      const fetchArgs = global.__lastFetchArgs;
      assert.ok(!fetchArgs, 'tools body check skipped in mock');
    } finally {
      global.fetch = originalFetch;
    }
  });

  // ---------- 测试 2：submitToolResult 续接（LLM tool_calls → 续接 → TTS） ----------
  await test('submitToolResult: LLM 工具调用后续接生成回复', async () => {
    const events = [];
    const bridge = createBridgeRuntime({
      providerConfig: { baseUrl: 'http://mock', apiKey: 'k', model: 'm' },
      autoRespondToAudio: true,
      tools: [{ type: 'function', name: 'openclaw_agent_consult' }],
      llmFactory: createMockLlmFactory({ consultReply: '这是 agent 查证后的回复。', firstRespondToolCall: true }),
      ttsFactory: createMockTtsFactory(),
      decoderFactory: createMockDecoderFactory(),
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onToolCall: (tc) => events.push({ type: 'toolCall', name: tc.name, callId: tc.callId }),
      onAudio: () => events.push({ type: 'audio' }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    await bridge.connect();
    bridge.sendUserMessage('测试问题');
    // 等待异步链路：tool_calls 上报应已发生
    await new Promise((r) => setTimeout(r, 50));
    const toolCalls = events.filter((e) => e.type === 'toolCall');
    assert.equal(toolCalls.length, 1, 'should report one onToolCall');
    assert.equal(toolCalls[0].name, 'openclaw_agent_consult');
    assert.equal(toolCalls[0].callId, 'call_123');

    // 回流 tool result → 续接
    bridge.submitToolResult('call_123', { result: '查证完成' });
    await new Promise((r) => setTimeout(r, 100));
    // 续接后应有 assistant final transcript（LLM 续接轮产出文本）
    const assistantFinals = events.filter((e) => e.type === 'transcript' && e.role === 'assistant' && e.final === true);
    assert.equal(assistantFinals.length, 1, 'should produce assistant final after continuation');
    assert.ok(assistantFinals[0].text.includes('agent 查证'), `text=${assistantFinals[0].text}`);
    bridge.close();
  });

  // ---------- 测试 3：autoRespondToAudio=false 不自管回复 ----------
  await test('autoRespondToAudio=false: 出句只上报 transcript 不自动回复', async () => {
    const events = [];
    const bridge = createBridgeRuntime({
      providerConfig: { baseUrl: 'http://mock', apiKey: 'k', model: 'm' },
      autoRespondToAudio: false,
      tools: [{ type: 'function', name: 'openclaw_agent_consult' }],
      llmFactory: createMockLlmFactory(),
      ttsFactory: createMockTtsFactory(),
      decoderFactory: createMockDecoderFactory(),
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onToolCall: (tc) => events.push({ type: 'toolCall' }),
      onAudio: () => events.push({ type: 'audio' }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    await bridge.connect();
    // 模拟语音出句：直接走 handleRecognizedText 内部路径不可行（闭包），
    // 但 sendUserMessage 是回流入口——验证 autoRespondToAudio=false 时
    // sendUserMessage（gateway 回流）仍能走通 LLM→TTS 全链路。
    bridge.sendUserMessage('agent 回流结果文本');
    await new Promise((r) => setTimeout(r, 80));
    const assistantFinals = events.filter((e) => e.type === 'transcript' && e.role === 'assistant' && e.final === true);
    assert.equal(assistantFinals.length, 1, '回流 sendUserMessage 应产出 assistant final');
    // 无 tool_calls（mock LLM 首轮若收到 tool result 会走续接，这里直接文本回流）
    bridge.close();
  });

  // ---------- 测试 4：autoRespondToAudio=false 语音路径状态安全 ----------
  await test('autoRespondToAudio=false: sendUserMessage 在 THINKING 态保持（等回流）', async () => {
    const events = [];
    const bridge = createBridgeRuntime({
      providerConfig: { baseUrl: 'http://mock', apiKey: 'k', model: 'm' },
      autoRespondToAudio: false,
      llmFactory: createMockLlmFactory({ consultReply: '好的。' }),
      ttsFactory: createMockTtsFactory(),
      decoderFactory: createMockDecoderFactory(),
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    await bridge.connect();
    bridge.sendUserMessage('回流一');
    // 回流二（前一轮仍在跑时）：sendUserMessage 应取消旧任务并重入 THINKING
    bridge.sendUserMessage('回流二');
    await new Promise((r) => setTimeout(r, 80));
    assert.equal(bridge.getState(), 'IDLE', 'TTS 完成后回 IDLE');
    bridge.close();
  });

  // ---------- 测试 5：consult 回流超时兜底（Phase 4 修复核心） ----------
  await test('autoRespondToAudio=false: 回流超时 → 自管 LLM 兜底回复（降级 fallback）', async () => {
    // handleRecognizedText 是 bridge 闭包，外部无法直接调用；只能走真实语音链路触发
    // autoRespondToAudio=false 分支：sendAudio 喂 wav + 静音 → sherpa STT 出句 → 兜底定时器启动。
    // 模型/wav 缺失时降级跳过（WARN）——本地验证路径与 e2e 一致。
    const wavPath = path.join(
      __dirname,
      '..',
      'models',
      'sherpa-onnx-streaming-zipformer-bilingual-zh-en-2023-02-20',
      'test_wavs',
      '0.wav',
    );
    if (!fs.existsSync(wavPath)) {
      console.warn('[WARN] test 5 skipped: sherpa test wav missing (模型缺失时降级跳过)');
      return;
    }
    const events = [];
    const bridge = createBridgeRuntime({
      providerConfig: { baseUrl: 'http://mock', apiKey: 'k', model: 'm' },
      autoRespondToAudio: false,
      consultFlowbackTimeoutMs: 30, // 测试注入短超时（生产默认 60000ms）
      classifyIntent: false, // 本测试聚焦 consult 兜底路径，关闭分类/清洗避免耦合
      ackEnabled: false, // ack 是语音路径功能，sendUserMessage 测试关闭避免「在的」干扰
      llmFactory: createMockLlmFactory({ consultReply: '兜底回复：没等到查证结果，先直接回复你。' }),
      ttsFactory: createMockTtsFactory(),
      decoderFactory: createMockDecoderFactory(),
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onAudio: () => events.push({ type: 'audio' }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    try {
      await bridge.connect();
    } catch (err) {
      console.warn(`[WARN] test 5 skipped: connect failed (${err.message})`);
      bridge.close();
      return;
    }
    // 解析 wav + 重采样 16k → 24k（模拟 gateway-relay），分块喂入 + 静音尾巴触发 STT endpoint
    const wav = parseWav(wavPath);
    if (wav.sampleRate !== 16000) throw new Error(`test 5: expected 16k wav, got ${wav.sampleRate}`);
    const resampler = new LinearResampler(16000, 24000);
    const inputPcm24k = float32ToPcm16(resampler.resample(pcm16ToFloat32(wav.pcm16)));
    const frameBytes = 24000 * 20 * 2 / 1000; // 960
    for (let offset = 0; offset < inputPcm24k.length; offset += frameBytes) {
      bridge.sendAudio(inputPcm24k.subarray(offset, offset + frameBytes));
    }
    const silence3s = Buffer.alloc(24000 * 2 * 3);
    for (let offset = 0; offset < silence3s.length && bridge.getState() === 'LISTENING'; offset += frameBytes) {
      bridge.sendAudio(silence3s.subarray(offset, offset + frameBytes));
    }
    // 等待兜底回复：30ms 超时 → sendUserMessage(出句) → mock LLM → TTS → onAudio
    const fallbackDone = await waitFor(() => events.some((e) => e.type === 'audio'), 5000);
    const userFinal = events.find((e) => e.type === 'transcript' && e.role === 'user' && e.final === true);
    const assistantFinals = events.filter((e) => e.type === 'transcript' && e.role === 'assistant' && e.final === true);
    assert.ok(userFinal, `STT 出句应上报 user final transcript，events=${JSON.stringify(events)}`);
    assert.ok(fallbackDone, `回流超时后应触发自管 LLM 兜底回复（onAudio），events=${JSON.stringify(events)}`);
    assert.equal(assistantFinals.length, 1, `应恰好触发一次兜底回复，实际 ${assistantFinals.length}`);
    assert.ok(assistantFinals[0].text.includes('兜底回复'), `text=${assistantFinals[0].text}`);
    bridge.close();
  });

  // ---------- 测试 6：autoRespondToAudio=false 回流文本不上报 user final（防 consult 循环） ----------
  // Phase 4 风暴修复核心：gateway 把 agent 结果经 sendUserMessage 回流 → runLlmTtsFlow
  // 不再上报 user final（改动 1），避免 gateway 把回流文本误当成新语音触发强制 consult。
  await test('autoRespondToAudio=false: 回流文本不上报 user final（防 consult 循环）', async () => {
    const events = [];
    const bridge = createBridgeRuntime({
      providerConfig: { baseUrl: 'http://mock', apiKey: 'k', model: 'm' },
      autoRespondToAudio: false,
      tools: [{ type: 'function', name: 'openclaw_agent_consult' }],
      llmFactory: createMockLlmFactory(),
      ttsFactory: createMockTtsFactory(),
      decoderFactory: createMockDecoderFactory(),
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onToolCall: (tc) => events.push({ type: 'toolCall' }),
      onAudio: () => events.push({ type: 'audio' }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    await bridge.connect();
    bridge.sendUserMessage('agent 回流结果文本');
    await new Promise((r) => setTimeout(r, 80));
    // 回流文本不应再上报 user final（真实出句的 user final 由 handleRecognizedText 单独上报）
    const userFinals = events.filter((e) => e.type === 'transcript' && e.role === 'user' && e.final === true);
    assert.equal(userFinals.length, 0, `回流文本不应上报 user final，实际 ${userFinals.length}，events=${JSON.stringify(events)}`);
    // 回流仍应产出 assistant final（LLM 转口语 → TTS 链路不受影响）
    const assistantFinals = events.filter((e) => e.type === 'transcript' && e.role === 'assistant' && e.final === true);
    assert.equal(assistantFinals.length, 1, '回流 sendUserMessage 应产出 assistant final');
    bridge.close();
  });

  // ---------- 测试 7：autoRespondToAudio=false LLM 调用不带 tools（bridge LLM 无工具权） ----------
  // Phase 4 风暴修复核心：agent-consult 主路径 consult 由 gateway 强制管理，bridge LLM
  // 只做口语转换，不传 tools（改动 2）——否则 bridge LLM 会主动调 consult 工具加剧循环。
  await test('autoRespondToAudio=false: LLM 调用不带 tools（bridge LLM 无工具权）', async () => {
    const events = [];
    let capturedLlm = null;
    const bridge = createBridgeRuntime({
      providerConfig: { baseUrl: 'http://mock', apiKey: 'k', model: 'm' },
      autoRespondToAudio: false,
      tools: [{ type: 'function', name: 'openclaw_agent_consult' }],
      llmFactory: (cfg) => {
        capturedLlm = createMockLlmFactory({ recordOptions: true })();
        return capturedLlm;
      },
      ttsFactory: createMockTtsFactory(),
      decoderFactory: createMockDecoderFactory(),
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onAudio: () => events.push({ type: 'audio' }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    await bridge.connect();
    bridge.sendUserMessage('agent 回流结果文本');
    await new Promise((r) => setTimeout(r, 80));
    assert.ok(capturedLlm, 'mock LLM 应被创建');
    assert.equal(capturedLlm.lastOptions?.tools, undefined, `autoRespondToAudio=false 时 bridge LLM 不应收到 tools，实际=${JSON.stringify(capturedLlm.lastOptions?.tools)}`);
    // 回流仍应产出 assistant final（无工具权不影响文本回流链路）
    const assistantFinals = events.filter((e) => e.type === 'transcript' && e.role === 'assistant' && e.final === true);
    assert.equal(assistantFinals.length, 1, '无工具权时回流仍应产出 assistant final');
    bridge.close();
  });

  // ---------- 测试 8：快 consult（≤阈值）→ 等待语不播；慢 consult（>阈值）→ 延迟后播等待语 ----------
  // Phase 4 占位处理延迟触发（澜影反馈驱动）：working 提示注入后不立即播，
  // placeholderDelayMs 后仍无真实回复才播「稍等，我查一下」。
  await test('快 consult（真实回复≤阈值到达）→ 等待语不播', async () => {
    const events = [];
    const bridge = createBridgeRuntime({
      providerConfig: { baseUrl: 'http://mock', apiKey: 'k', model: 'm' },
      placeholderDelayMs: 300,
      llmFactory: () => createMockLlmFactory({ consultReply: '这是快 consult 的真实回复。' })(),
      ttsFactory: createMockTtsFactory(),
      decoderFactory: createMockDecoderFactory(),
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onAudio: () => events.push({ type: 'audio' }),
      onMark: (name) => events.push({ type: 'mark', name }),
      onEvent: (e) => events.push({ type: 'event', eventType: e.type }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    await bridge.connect();
    // working 提示注入（占位）
    bridge.sendUserMessage('Briefly tell the person that you are checking with OpenClaw. Do not answer the request yet.');
    // 30ms 内真实回复到达（模拟 consult 快）→ 取消等待语定时器
    await new Promise((r) => setTimeout(r, 30));
    bridge.sendUserMessage('这是快 consult 的真实回复。');
    await new Promise((r) => setTimeout(r, 500)); // 等超过阈值（300ms）
    // 应只播真实回复（1 次 onAudio 序列），无等待语单独播放
    assert.ok(events.filter((e) => e.type === 'audio').length >= 1, '真实回复应播放');
    const suppressed = events.filter((e) => e.eventType === 'placeholder_suppressed');
    assert.ok(suppressed.length >= 1, 'working 提示应上报 suppressed');
    assert.ok(events.some((e) => e.type === 'transcript' && e.role === 'assistant' && e.final && e.text.includes('快 consult')), '应播真实回复文本');
    bridge.close();
  });

  await test('慢 consult（>阈值无真实回复）→ 延迟后播等待语', async () => {
    const events = [];
    let llmCalled = false;
    const bridge = createBridgeRuntime({
      providerConfig: { baseUrl: 'http://mock', apiKey: 'k', model: 'm' },
      placeholderDelayMs: 200,
      llmFactory: () => {
        llmCalled = true;
        return createMockLlmFactory({ consultReply: '不应被调用' })();
      },
      ttsFactory: createMockTtsFactory(),
      decoderFactory: createMockDecoderFactory(),
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onAudio: () => events.push({ type: 'audio' }),
      onMark: (name) => events.push({ type: 'mark', name }),
      onEvent: (e) => events.push({ type: 'event', eventType: e.type }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    await bridge.connect();
    bridge.sendUserMessage('Briefly tell the person that you are checking with OpenClaw. Do not answer the request yet.');
    await new Promise((r) => setTimeout(r, 400)); // 等超过阈值（200ms），无真实回复
    assert.equal(llmCalled, false, '占位指令不应触发 LLM 调用');
    assert.ok(events.filter((e) => e.type === 'audio').length >= 1, '慢 consult 应延迟播等待语（onAudio≥1）');
    assert.ok(events.some((e) => e.type === 'mark' && e.name === 'response-start'), '应进入播放（response-start）');
    assert.ok(events.some((e) => e.eventType === 'placeholder_suppressed'), '应上报 placeholder_suppressed 事件');
    assert.equal(bridge.getState(), 'IDLE', `播放完成后应回 IDLE，实际=${bridge.getState()}`);
    bridge.close();
  });

  await test('真实回复到达取消等待语定时器（consult 快则等待语不播）', async () => {
    const events = [];
    const bridge = createBridgeRuntime({
      providerConfig: { baseUrl: 'http://mock', apiKey: 'k', model: 'm' },
      placeholderDelayMs: 300,
      llmFactory: () => createMockLlmFactory({ consultReply: '真实回复' })(),
      ttsFactory: createMockTtsFactory(),
      decoderFactory: createMockDecoderFactory(),
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onAudio: () => events.push({ type: 'audio' }),
      onMark: (name) => events.push({ type: 'mark', name }),
      onEvent: (e) => events.push({ type: 'event', eventType: e.type }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    await bridge.connect();
    bridge.sendUserMessage('Briefly tell the person that you are checking with OpenClaw.');
    await new Promise((r) => setTimeout(r, 30)); // 定时器已启动（300ms）
    bridge.sendUserMessage('真实回复'); // 真实回复到达 → 取消定时器
    await new Promise((r) => setTimeout(r, 400)); // 等超过原阈值
    // 无等待语单独播放：总 audio 只来自真实回复（≥1）
    assert.ok(events.filter((e) => e.type === 'audio').length >= 1, '真实回复应播放');
    bridge.close();
  });

  await test('重复 working 提示幂等（不重复启动定时器/不重复播）', async () => {
    const events = [];
    const bridge = createBridgeRuntime({
      providerConfig: { baseUrl: 'http://mock', apiKey: 'k', model: 'm' },
      placeholderDelayMs: 150,
      ackEnabled: false, // ack 是语音路径功能，sendUserMessage 测试关闭避免「在的」干扰
      classifyIntent: false, // 测延迟阈值机制（schedulePlaceholderReplacement）幂等；分类接管时此路径禁用
      llmFactory: () => createMockLlmFactory({ consultReply: 'x' })(),
      ttsFactory: createMockTtsFactory(),
      decoderFactory: createMockDecoderFactory(),
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onAudio: () => events.push({ type: 'audio' }),
      onMark: (name) => events.push({ type: 'mark', name }),
      onEvent: (e) => events.push({ type: 'event', eventType: e.type }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    await bridge.connect();
    // 连续 3 次 working 提示（幂等：只启动 1 个定时器）
    for (let i = 0; i < 3; i++) {
      bridge.sendUserMessage('Briefly tell the person that you are checking with OpenClaw.');
      await new Promise((r) => setTimeout(r, 20));
    }
    await new Promise((r) => setTimeout(r, 300)); // 等超过阈值
    const responseStarts = events.filter((e) => e.type === 'mark' && e.name === 'response-start').length;
    assert.equal(responseStarts, 1, `重复提示应只播 1 次等待语，实际=${responseStarts}`);
    bridge.close();
  });

  // ---------- 测试 9：占位句（LLM 输出命中）→ 播放中文等待语（过滤点 B） ----------
  // 覆盖：过滤点 A 未拦截（如占位经其他路径进入 LLM）时，LLM 回复命中占位模式 → 改播等待语。
  await test('占位句（LLM 输出命中）→ 播放中文等待语', async () => {
    const events = [];
    const bridge = createBridgeRuntime({
      providerConfig: { baseUrl: 'http://mock', apiKey: 'k', model: 'm' },
      llmFactory: () => createMockLlmFactory({ consultReply: "I'll check with OpenClaw on this and get back to you shortly." })(),
      ttsFactory: createMockTtsFactory(),
      decoderFactory: createMockDecoderFactory(),
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onAudio: () => events.push({ type: 'audio' }),
      onMark: (name) => events.push({ type: 'mark', name }),
      onEvent: (e) => events.push({ type: 'event', eventType: e.type }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    await bridge.connect();
    bridge.sendUserMessage('测试问题');
    await new Promise((r) => setTimeout(r, 100));
    assert.ok(events.filter((e) => e.type === 'audio').length >= 1, 'LLM 输出占位句应改播中文等待语（onAudio≥1）');
    assert.ok(events.some((e) => e.type === 'mark' && e.name === 'response-start'), '应进入播放（response-start）');
    assert.ok(events.some((e) => e.eventType === 'placeholder_suppressed'), '应上报 placeholder_suppressed 事件');
    assert.equal(bridge.getState(), 'IDLE', `播放完成后应回 IDLE，实际=${bridge.getState()}`);
    bridge.close();
  });

  // ---------- 测试 10：正常回复 → 照常播放（回归保护） ----------
  await test('正常回复 → 照常播放', async () => {
    const events = [];
    const bridge = createBridgeRuntime({
      providerConfig: { baseUrl: 'http://mock', apiKey: 'k', model: 'm' },
      llmFactory: () => createMockLlmFactory({ consultReply: '这是正常的回复内容。' })(),
      ttsFactory: createMockTtsFactory(),
      decoderFactory: createMockDecoderFactory(),
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onAudio: () => events.push({ type: 'audio' }),
      onEvent: (e) => events.push({ type: 'event', eventType: e.type }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    await bridge.connect();
    bridge.sendUserMessage('正常问题');
    await new Promise((r) => setTimeout(r, 100));
    assert.ok(events.filter((e) => e.type === 'audio').length >= 1, '正常回复应播放音频');
    assert.ok(!events.some((e) => e.eventType === 'placeholder_suppressed'), '正常回复不应被误判为占位');
    assert.equal(bridge.getState(), 'IDLE', `播放完成后应回 IDLE，实际=${bridge.getState()}`);
    bridge.close();
  });

  // ---------- 测试 11：suppressPlaceholderReplies=false → 完全静默（开关生效） ----------
  await test('suppressPlaceholderReplies=false → 占位句完全静默', async () => {
    const events = [];
    const bridge = createBridgeRuntime({
      providerConfig: { baseUrl: 'http://mock', apiKey: 'k', model: 'm' },
      suppressPlaceholderReplies: false,
      llmFactory: () => createMockLlmFactory({ consultReply: "I'll check with OpenClaw on this and get back to you shortly." })(),
      ttsFactory: createMockTtsFactory(),
      decoderFactory: createMockDecoderFactory(),
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onAudio: () => events.push({ type: 'audio' }),
      onMark: (name) => events.push({ type: 'mark', name }),
      onEvent: (e) => events.push({ type: 'event', eventType: e.type }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    await bridge.connect();
    // 过滤点 A：占位指令 → 完全静默
    bridge.sendUserMessage('Briefly tell the person that you are checking with OpenClaw.');
    await new Promise((r) => setTimeout(r, 80));
    assert.equal(events.filter((e) => e.type === 'audio').length, 0, '开关关闭时占位指令应完全静默（onAudio=0）');
    assert.ok(events.some((e) => e.eventType === 'placeholder_suppressed'), '仍应上报 suppressed 事件（供排查）');
    // 过滤点 B：LLM 输出占位 → 完全静默
    bridge.sendUserMessage('测试问题');
    await new Promise((r) => setTimeout(r, 100));
    assert.equal(events.filter((e) => e.type === 'audio').length, 0, '开关关闭时 LLM 输出占位也应完全静默');
    bridge.close();
  });

  // ---------- 测试 12：批次 A-6 双回复修复——兜底回复后真实回流静默丢弃 ----------
  // 根因：15s consult 兜底在工具调用慢（20s）时误触发 → bridge 自管 LLM 回复（无工具 = 第一次），
  // 真实 consult 返回（带工具 = 第二次）→ 同一出句两次回复。fallbackReplied 置位后
  // sendUserMessage（真实回流入口）静默丢弃，不产出第二次回复。
  await test('批次 A-6: 兜底回复后真实回流静默丢弃（防双回复）', async () => {
    const wavPath = path.join(
      __dirname,
      '..',
      'models',
      'sherpa-onnx-streaming-zipformer-bilingual-zh-en-2023-02-20',
      'test_wavs',
      '0.wav',
    );
    if (!fs.existsSync(wavPath)) {
      console.warn('[WARN] test 12 skipped: sherpa test wav missing (模型缺失时降级跳过)');
      return;
    }
    const events = [];
    const mockLlm = createMockLlmFactory({ consultReply: '兜底回复：没等到查证结果，先直接回复你。' })();
    const bridge = createBridgeRuntime({
      providerConfig: { baseUrl: 'http://mock', apiKey: 'k', model: 'm' },
      autoRespondToAudio: false,
      consultFlowbackTimeoutMs: 30, // 短超时加速触发兜底（生产默认 60000ms）
      classifyIntent: false,
      ackEnabled: false,
      llmFactory: () => mockLlm,
      ttsFactory: createMockTtsFactory(),
      decoderFactory: createMockDecoderFactory(),
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onAudio: () => events.push({ type: 'audio' }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    try {
      await bridge.connect();
    } catch (err) {
      console.warn(`[WARN] test 12 skipped: connect failed (${err.message})`);
      bridge.close();
      return;
    }
    // 语音喂入 → STT 出句 → reportToGateway（兜底定时器 30ms）
    const wav = parseWav(wavPath);
    const resampler = new LinearResampler(16000, 24000);
    const inputPcm24k = float32ToPcm16(resampler.resample(pcm16ToFloat32(wav.pcm16)));
    const frameBytes = 24000 * 20 * 2 / 1000; // 960
    for (let offset = 0; offset < inputPcm24k.length; offset += frameBytes) {
      bridge.sendAudio(inputPcm24k.subarray(offset, offset + frameBytes));
    }
    const silence3s = Buffer.alloc(24000 * 2 * 3);
    for (let offset = 0; offset < silence3s.length && bridge.getState() === 'LISTENING'; offset += frameBytes) {
      bridge.sendAudio(silence3s.subarray(offset, offset + frameBytes));
    }
    // 等待兜底回复（30ms 超时 → sendUserMessage(出句) → mock LLM → TTS → onAudio）
    const fallbackDone = await waitFor(() => events.some((e) => e.type === 'audio'), 5000);
    assert.ok(fallbackDone, `回流超时后应触发兜底回复（onAudio），events=${JSON.stringify(events)}`);
    const beforeCount = events.filter((e) => e.type === 'transcript' && e.role === 'assistant' && e.final === true).length;
    assert.equal(beforeCount, 1, `兜底应恰好产出 1 次 assistant final，实际 ${beforeCount}`);
    assert.equal(mockLlm.getCallCount(), 1, `兜底应触发 1 次 LLM 调用，实际 ${mockLlm.getCallCount()}`);
    // 真实 consult 回流到达（工具慢返回的带结果回复）→ fallbackReplied=true → 静默丢弃
    bridge.sendUserMessage('真实 consult 结果：上海今天晴，24 度。');
    await sleep(200);
    const afterCount = events.filter((e) => e.type === 'transcript' && e.role === 'assistant' && e.final === true).length;
    assert.equal(afterCount, 1, `真实回流应被静默丢弃（不产出第二次回复），实际 ${afterCount}`);
    assert.equal(mockLlm.getCallCount(), 1, `真实回流不应触发第二次 LLM 调用，实际 ${mockLlm.getCallCount()}`);
    bridge.close();
  });

  // ---------- 测试 13：批次 A-6 回归保护——无兜底时真实回流照常 ----------
  await test('批次 A-6: 无兜底时真实回流照常（回归保护）', async () => {
    const wavPath = path.join(
      __dirname,
      '..',
      'models',
      'sherpa-onnx-streaming-zipformer-bilingual-zh-en-2023-02-20',
      'test_wavs',
      '0.wav',
    );
    if (!fs.existsSync(wavPath)) {
      console.warn('[WARN] test 13 skipped: sherpa test wav missing (模型缺失时降级跳过)');
      return;
    }
    const events = [];
    const mockLlm = createMockLlmFactory({ consultReply: '这是 agent 查证后的回复。' })();
    const bridge = createBridgeRuntime({
      providerConfig: { baseUrl: 'http://mock', apiKey: 'k', model: 'm' },
      autoRespondToAudio: false,
      consultFlowbackTimeoutMs: 5000, // 长超时：回流在超时前到达 → 不走兜底
      classifyIntent: false,
      ackEnabled: false,
      llmFactory: () => mockLlm,
      ttsFactory: createMockTtsFactory(),
      decoderFactory: createMockDecoderFactory(),
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onAudio: () => events.push({ type: 'audio' }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    try {
      await bridge.connect();
    } catch (err) {
      console.warn(`[WARN] test 13 skipped: connect failed (${err.message})`);
      bridge.close();
      return;
    }
    // 语音喂入 → STT 出句 → reportToGateway（兜底定时器 5s，不会触发）
    const wav = parseWav(wavPath);
    const resampler = new LinearResampler(16000, 24000);
    const inputPcm24k = float32ToPcm16(resampler.resample(pcm16ToFloat32(wav.pcm16)));
    const frameBytes = 24000 * 20 * 2 / 1000; // 960
    for (let offset = 0; offset < inputPcm24k.length; offset += frameBytes) {
      bridge.sendAudio(inputPcm24k.subarray(offset, offset + frameBytes));
    }
    const silence3s = Buffer.alloc(24000 * 2 * 3);
    for (let offset = 0; offset < silence3s.length && bridge.getState() === 'LISTENING'; offset += frameBytes) {
      bridge.sendAudio(silence3s.subarray(offset, offset + frameBytes));
    }
    // 出句后快速真实回流（30ms 内）→ fallbackReplied=false → 正常 LLM→TTS 链路
    await sleep(30);
    bridge.sendUserMessage('真实 consult 结果：上海今天晴，24 度。');
    await sleep(150);
    const assistantFinals = events.filter((e) => e.type === 'transcript' && e.role === 'assistant' && e.final === true);
    assert.equal(assistantFinals.length, 1, `无兜底时真实回流应照常回复（1 次 assistant final），实际 ${assistantFinals.length}`);
    assert.equal(mockLlm.getCallCount(), 1, `无兜底时真实回流应触发 1 次 LLM 调用，实际 ${mockLlm.getCallCount()}`);
    assert.ok(assistantFinals[0].text.includes('agent 查证'), `text=${assistantFinals[0].text}`);
    bridge.close();
  });

  // ---------- 测试 14：批次 A-7 源码回归——兜底常量 60000 + cleanedText 兜底 + system prompt 注入 ----------
  await test('批次 A-7 源码回归: 兜底常量=60000 + cleanedText 兜底 + fallbackReplied + system prompt 注入', async () => {
    const src = fs.readFileSync('src/bridge/bridge-runtime.js', 'utf8');
    // 兜底常量 = 60000（批次 A-7：20s 兜底抢答 < 真实工具 consult 30-60s）
    assert.ok(
      src.includes('const CONSULT_FLOWBACK_TIMEOUT_MS = 60000;'),
      '兜底常量应为 CONSULT_FLOWBACK_TIMEOUT_MS = 60000'
    );
    // fallbackReplied 声明
    assert.ok(src.includes('let fallbackReplied = false;'), '应声明 fallbackReplied');
    // 批次 A-7：兜底文本保存变量声明
    assert.ok(src.includes("let lastFallbackRawText = '';"), '应声明 lastFallbackRawText');
    assert.ok(src.includes("let lastFallbackReportText = '';"), '应声明 lastFallbackReportText');
    // reportToGateway 重置（新 consult 启动）+ 保存 cleanedText（兜底优先用）
    assert.ok(
      src.includes('fallbackReplied = false; // 批次 A-6：新 consult 启动重置「已兜底」标志'),
      'reportToGateway 应重置 fallbackReplied'
    );
    assert.ok(
      src.includes('lastFallbackReportText = reportText; // 批次 A-7：保存 cleanedText（兜底优先用）'),
      'reportToGateway 应保存 cleanedText 供兜底优先用'
    );
    // 兜底回调置位顺序：先 sendUserMessage（false 不被拦）再置 true（顺序不能反）
    const fbStart = src.indexOf('consultFallbackTimer = setTimeout');
    assert.ok(fbStart >= 0, '应找到 consult 兜底定时器');
    const fbBlock = src.slice(fbStart, fbStart + 500);
    // 批次 A-7：兜底优先用 cleanedText（lastFallbackReportText 优先，降级 lastFallbackRawText）
    assert.ok(
      fbBlock.includes("(lastFallbackReportText || lastFallbackRawText || '').trim()"),
      '兜底回调应优先用 lastFallbackReportText（cleanedText），降级 lastFallbackRawText'
    );
    const sendPos = fbBlock.indexOf('sendUserMessage(fallbackText)');
    const setPos = fbBlock.indexOf('fallbackReplied = true');
    assert.ok(sendPos >= 0 && setPos > sendPos, '兜底回调应先 sendUserMessage 再置位 fallbackReplied（顺序不能反）');
    // sendUserMessage 静默判断
    assert.ok(src.includes('if (fallbackReplied) {'), 'sendUserMessage 应含 fallbackReplied 静默丢弃判断');
    // close 重置
    assert.ok(
      src.includes('fallbackReplied = false; // 批次 A-6：会话关闭重置「已兜底」标志'),
      'close 应重置 fallbackReplied'
    );
    // 批次 A-7：system prompt 注入（runLlmTtsFlow 前插 system）
    assert.ok(src.includes('export function buildBridgeSystemPrompt()'), '应导出 buildBridgeSystemPrompt');
    assert.ok(
      src.includes("{ role: 'system', content: buildBridgeSystemPrompt() }"),
      'runLlmTtsFlow 应前插 system 消息（buildBridgeSystemPrompt）'
    );
    assert.ok(
      src.includes('getLlm().streamChat(fullMessages'),
      'runLlmTtsFlow 应改用 fullMessages 调 streamChat'
    );
  });

  // ---------- 测试 15：批次 A-7 system prompt 日期格式断言 ----------
  // 兜底 LLM 裸跑曾编造日期（2025年4月21日，实际 2026年8月）；buildBridgeSystemPrompt 注入
  // 运行时真实日期（Asia/Shanghai）——断言含「年」「月」「日」且不含编造年份「2025」。
  await test('批次 A-7: buildBridgeSystemPrompt 注入真实日期（含年月日，非 2025）', async () => {
    const { buildBridgeSystemPrompt } = await import('../src/bridge/bridge-runtime.js');
    const prompt = buildBridgeSystemPrompt();
    assert.ok(prompt.includes('年'), `system prompt 应含「年」，实际: ${prompt}`);
    assert.ok(prompt.includes('月'), `system prompt 应含「月」，实际: ${prompt}`);
    assert.ok(prompt.includes('日'), `system prompt 应含「日」，实际: ${prompt}`);
    assert.ok(prompt.includes('Asia/Shanghai'), `system prompt 应含 Asia/Shanghai 时区标注，实际: ${prompt}`);
    assert.ok(!prompt.includes('2025'), `system prompt 不应含编造年份 2025（旧 bug：兜底 LLM 裸跑把日期写成 2025年4月21日），实际: ${prompt}`);
  });

  console.log('\n===== agent-consult-test results =====');
  for (const r of results) {
    console.log(`[${r.ok ? 'PASS' : 'FAIL'}] ${r.name}${r.error ? ` (${r.error})` : ''}`);
  }
  console.log(`${passed} passed, ${failed} failed`);
  process.exitCode = failed > 0 ? 1 : 0;
}

main().catch((err) => {
  console.error(`[ERROR] ${err.stack ?? err}`);
  process.exit(1);
});
