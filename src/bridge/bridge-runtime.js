// src/bridge/bridge-runtime.js
// createBridge 核心逻辑（Phase 2）：状态机编排 + 输入音频管道 + LLM+TTS 输出链路。
// 入口薄层：index.js 只做参数解析 + 回调透传，业务逻辑全部在本模块。
// 状态流转（Phase 2 全链路）：
//   IDLE --INPUT_START--> LISTENING --SPEECH_END--> RECOGNIZING --RECOGNIZED--> THINKING
//   THINKING --TTS_START--> SPEAKING --TTS_END--> IDLE
//   IDLE --TEXT_INPUT--> THINKING（SDK sendUserMessage 文本输入路径）
//   BARGE_IN 任意活动态 --> LISTENING（管道保持、打断后继续听；LLM/TTS 异步任务被取消）
// 链路：sendUserMessage(text) → LLM streamChat → 累积全文 → edge-tts 合成 → mpg123 解码
//       → 20ms pcm16 帧 → onAudio 推送（Control UI playhead 契约）。

import { Buffer } from 'node:buffer';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { STATES, EVENTS, transition, canTransition } from './state-machine.js';
import { createPipeline } from './audio-pipeline.js';
import { createStt } from '../stt/sherpa-stt.js';
import { createOpenAiClient } from '../llm/openai-client.js';
import { createEdgeTts } from '../tts/edge-tts.js';
import { createDecodePipeline, FRAME_BYTES } from '../tts/decode-pipeline.js';
import { createPresynthCache } from '../tts/presynth-cache.js';

// sherpa 流式模型目录（插件 models/ 下，dev-plan 固定布局；providerConfig.modelPath 可覆盖）。
const DEFAULT_MODEL_PATH = join(
  dirname(fileURLToPath(import.meta.url)),
  '..',
  '..',
  'models',
  'sherpa-onnx-streaming-zipformer-bilingual-zh-en-2023-02-20',
);

// 输入 PCM 采样率兜底：SDK 硬约束 RealtimeVoiceAudioFormat 只允许 pcm16/24000 或
// g711_ulaw/8000；gateway-relay 会把浏览器 48k 重采样到声明的 24k，故兜底用 24k
//（Phase 3 采样率实测对齐结论：48k 非原生格式，插件侧保证收到 24k PCM16）。
const DEFAULT_INPUT_SAMPLE_RATE = 24000;

// LLM 配置默认值（OpenAI 兼容端点；baseUrl 兼容 DeepSeek 官方当前无 /v1 前缀，
// 但保留 /v1 兼容旧端点；model 占位 deepseek-chat 待影裁决，configSchema 可配置）。
const DEFAULT_LLM_CONFIG = Object.freeze({
  baseUrl: 'https://api.deepseek.com/v1',
  apiKey: '',
  model: 'deepseek-chat',
});

// Phase 4 consult 回流等待超时：autoRespondToAudio=false 时 bridge 不自管回复，等 gateway
// 强制 consult 回流（agent 结果经 sendUserMessage 回流）。实测 consult 场景 deepseek-v4-flash
// 只产 thinking 无 content → 前端 no text → 无回流。超时后 bridge 自管 LLM 直接回复该出句，
// 保证用户总能听到回复（澜影「bridge LLM 保留作降级 fallback」裁决落地）。
// 批次 A-7：20s → 60s。工具 consult 正常耗时 30-60s（gateway 强制 consult 执行期零中间信号
// 回流 bridge，见诊断：bridge 只能感知 handleBargeIn(force) 发起 + checking prompt 一次性提示），
// 20s 兜底 < 真实 consult 时长 → 兜底抢答、真实答案被静默丢弃（fallbackReplied=true）。
// 60s 覆盖正常工具 consult 上限；仍超时（consult 真挂）才兜底。若实测 consult 恰好 60s
// 仍有抢答边界风险，再提至 75-90s。
const CONSULT_FLOWBACK_TIMEOUT_MS = 60000;

// Phase 4 回声规避（方案 B 折中版播放锁）：TTS 播放结束后追加的静默抑制窗，
// 防扬声器余音/回声尾巴触发误识别出句。可经 createBridgeRuntime 注入调参。
const PLAYBACK_TAIL_SUPPRESSION_MS = 300;

// Phase 4 consult 英文占位句处理（澜影拍板升级）：
// gateway 强制 consult 时，buildForcedConsultCheckingPrompt()（英文指令「Briefly tell the
// person that you are checking with OpenClaw...」）经 sendUserMessage 注入 bridge，
// bridge 自管 LLM 依从生成英文占位（「I'll check with OpenClaw...」）→ TTS 播放出戏。
// 检测到占位模式 → 播放中文等待语（默认「稍等，我查一下」，可配置），不播英文占位。
// 语义：consult 处理中（working，工具可能被调）→ 播等待语告知用户；无实际内容 → 走 60s
// 兜底路径（bridge 自管 LLM 中文回复）。suppressPlaceholderReplies=false → 完全静默。
// 覆盖两类：gateway 指令文本 + LLM 生成占位。可经 createBridgeRuntime 覆盖。
const DEFAULT_PLACEHOLDER_PATTERNS = Object.freeze([
  /checking with OpenClaw/i,
  /briefly tell the person/i,
  /I'?ll check with OpenClaw/i,
  /I will check with OpenClaw/i,
  /check with OpenClaw/i,
]);

// 占位句替换的中文等待语（可经 createBridgeRuntime 注入调参）。
const DEFAULT_PLACEHOLDER_REPLACEMENT_TEXT = '稍等，我查一下';

// 中文等待语延迟触发阈值（可经 createBridgeRuntime 注入调参）：
// gateway checking prompt（consult working 提示）注入后不立即播，延迟该毫秒数仍无真实回复
// 才播等待语——consult 快（≤阈值）直接播真实回复，慢（>阈值）播等待语防焦虑。
const DEFAULT_PLACEHOLDER_DELAY_MS = 3000;

// 方案 3.0 意图分类超时预算：分类 LLM 调用最多等待该毫秒数，超时未返回 → 降级立即播等待语
//（宁可多提示不可干等）。本地实测分类 1.3-2s，但 gateway 环境长尾达 8.9-17.7s
//（进程内排队/网络/并发）→ 默认 12000ms 覆盖长尾；极端仍降级（上报原始 + 播等待语，不恶化）。
const DEFAULT_CLASSIFY_TIMEOUT_MS = 12000;

// 方案 3.0 意图分类器提示词（可经 createBridgeRuntime 注入）：三任务 JSON 输出——
// 意图分类（是否需要外部信息/工具）+ 语音转录顺化（去重复字/修正同音错字/补顺语序，
// 保守改写：不改语义/数字/地名/专名，不润色）+ 可理解性判断（转录是否可还原用户意图）。
// 只输出固定 schema JSON。
const DEFAULT_INTENT_CLASSIFIER_PROMPT =
  '你是语音助手的中文转录清洗与意图分类器。用户语音转录可能含重复字、同音错字、语序颠倒。' +
  '任务1（文本顺化）：修正转录错误为通顺中文——去重复字（如「天天天天津」→「天津」）、' +
  '修正同音错字（如「腕断了」→「打断了」）、补顺语序。保守改写：不改语义、不改数字/地名/专名，' +
  '不润色、不扩写。任务2（意图分类）：判断顺化后请求是否需要外部信息或工具（天气、搜索、查资料、计算、查询、联网才能回答的内容）。' +
  '任务3（可理解性）：判断转录是否能还原用户可执行的意图。连续重复且无语义、纯噪音（如「啊吧啦呼」）→ false；' +
  '简短输入（「嗯」「好」「在呢」）语义完整 → true；无法判断 → true（宽松，不打断用户）。' +
  '只输出 JSON，schema 固定：{"needTool": true或false, "cleanedText": "顺化后的文本", "understandable": true或false}。' +
  '不要输出 JSON 之外任何内容。';

/**
 * 批次 A-7：bridge 自管 LLM 的 system prompt——注入运行时真实日期（Asia/Shanghai 防编造）、
 * 口语化简短回答、顺化重复字、保守改写（不改语义/数字/地名/专名）、不知道就明说。
 * 只作用于 bridge 自管回复路径（兜底/autoRespondToAudio/consult 回流口语转换），
 * 不污染 gateway consult 链路（forced consult 上下文由 OpenClaw 侧构建）。
 * 导出供测试断言日期格式（含「年」「月」「日」且不含编造年份如「2025」）。
 */
export function buildBridgeSystemPrompt() {
  const now = new Date();
  const dateStr = new Intl.DateTimeFormat('zh-CN', {
    timeZone: 'Asia/Shanghai',
    year: 'numeric',
    month: 'long',
    day: 'numeric',
    weekday: 'long',
  }).format(now);
  return (
    '你是语音助手的中文口语回复模块。今天是 ' + dateStr + '（Asia/Shanghai 时区）。' +
    '将输入转换为口语化、简短的中文回答（1-3 句）。' +
    '顺化转录重复字（如「今天天天天天气」→「今天天气」）。' +
    '保守改写：不改语义、数字、地名、专名。' +
    '不知道就明确说「我不知道」，绝不编造。'
  );
}

// 转录不可理解时的重说提示文案（可经 createBridgeRuntime 注入调参）。
const DEFAULT_RETRY_PROMPT_TEXT = '抱歉没听清，请再说一次';

// ack 两层反馈（澜影拍板，砍掉「嗯」）：
// ① 点 Talk 启动播「在的」（readyText）——系统就绪，请说话
// ② 工具调用播「稍等，我查一下」（placeholderReplacementText，等待语）
// 「嗯」无应用场景：轻任务几秒有回复、重任务有等待语、中间态是平台 bug 该修、
// Control UI 视觉状态已覆盖收到反馈。
const DEFAULT_READY_TEXT = '在的';

// 批次 A-4 方案 B：等待语播前静默窗（ms）——等用户尾音/扬声器回声衰减，降低
// 消费端 barge-in 检测（RMS≥0.02 连续 2 帧）把回声误判为打断的概率。
const PRESYNTH_PLAY_PRE_DELAY_MS = 300;

// 批次 B：合法单字应答白名单（最短 utterance 过滤保留的单字）。
// 单字符且不在白名单的 transcript 视为环境/回声噪音误触发，直接丢弃不出句。
const SINGLE_CHAR_WHITELIST = Object.freeze(['嗯', '好', '在', '是', '对', '行', '唉']);

/** 批次 B：单字符且不在白名单 → 视为噪音（环境/回声误触发），忽略不出句。 */
export function isIgnorableUtterance(text) {
  if (typeof text !== 'string' || text.length === 0) return false;
  return text.length === 1 && !SINGLE_CHAR_WHITELIST.includes(text);
}

/** 占位句命中判定（供过滤点 A/B 共用）：命中任一模式即视为 consult 占位提示。 */
function isPlaceholderText(text, patterns) {
  if (typeof text !== 'string' || text.length === 0) return false;
  return patterns.some((pattern) => {
    try {
      return pattern instanceof RegExp ? pattern.test(text) : text.includes(pattern);
    } catch {
      return false;
    }
  });
}

/**
 * 解析 providerConfig.apiKey：可能为字符串（测试/直接注入）或 OpenClaw env 引用对象
 * {source:'env', provider:'default', id:'OPENCODE_API_KEY'}（gateway 透传原始 config，
 * 不解析 env）→ 从 process.env[id] 取实际值；解析失败返回空串（createOpenAiClient 会清晰报错）。
 * @param {unknown} apiKey
 * @returns {string}
 */
export function resolveApiKey(apiKey) {
  if (typeof apiKey === 'string') return apiKey;
  if (apiKey && typeof apiKey === 'object' && apiKey.source === 'env' && typeof apiKey.id === 'string') {
    return process.env[apiKey.id] ?? '';
  }
  return '';
}

/**
 * 转录输入预处理（方案 1）：连续相同字符 >3 个压缩为 1 个。
 * 目的：极端重复字（「阿爸阿啊叭叭啊八阿爸阿阿阿阿爸…」几十个重复）送分类 LLM 前
 * 先本地规约，缩短输入、减轻 LLM 负担、从根上减少 gateway 环境分类超时。
 * 规则保守：只压「>3 个连续相同」才压（「天津」的"津"不压；「津津」2 个连续不压；
 * 「津津津津」4 个 → 1 个）——不碰正常词，不误伤。
 * 注：语音转录的重复多为同音多字（阿/爸/叭），此函数处理同字连续重复；不同字交替重复
 * 由 LLM 顺化（prompt 已含合并重复指令）。
 * @param {string} text
 * @returns {string}
 */
export function collapseRepeatedChars(text) {
  if (typeof text !== 'string' || text.length === 0) return text;
  let out = '';
  let runChar = '';
  let runLen = 0;
  for (const ch of text) {
    if (ch === runChar) {
      runLen += 1;
      if (runLen <= 3) {
        out += ch; // 第 2/3 个连续相同：保留（<4 个不压）
      } else if (runLen === 4) {
        // 第 4 个连续相同：把前 3 个删掉，只留 1 个（压缩到 1）
        out = out.slice(0, -3);
        out += ch;
      }
      // 第 5+ 个连续相同：跳过（已压到 1）
    } else {
      runChar = ch;
      runLen = 1;
      out += ch;
    }
  }
  return out;
}

/**
 * 批次 A-4：presynth buffer 推帧 async generator——分 3-4 批推帧，批间 interval 间隔，
 * 模拟真实 TTS 的 chunk 到达节奏，让消费端「每批 audio 到达重置 barge-in 恢复标志」；
 * 批内帧微任务连推。既不是同步爆发（消费端只处理首帧），也不是 setTimeout 慢推
 * （消费端丢弃整个片段）。用 export 便于单测。
 * @param {Buffer} pcm - pcm16 buffer
 * @param {number} frameSize - 每帧字节数（960 = 20ms@24k）
 * @param {number} [batchIntervalMs] - 批间间隔（默认 50ms；批次 A-6 由 300ms 缩短——
 *   消除分批暂停造成的播放断流卡顿，同时保留分批推帧让消费端「每批到达重置 barge-in 标志」）
 */
export async function* presynthFrames(pcm, frameSize, batchIntervalMs = 50) {
  const frames = [];
  for (let offset = 0; offset < pcm.length; offset += frameSize) {
    frames.push(pcm.subarray(offset, offset + frameSize));
  }
  // 批次 A-4 方案 A：分 3-4 批推帧，批间 interval 间隔——模拟真实 TTS 的 chunk 到达
  // 节奏，让消费端「每批 audio 到达重置 barge-in 恢复标志」；批内帧微任务连推。
  const batchCount = Math.min(4, Math.max(1, Math.ceil(frames.length / 20)));
  const batchSize = Math.ceil(frames.length / batchCount);
  for (let i = 0; i < frames.length; i += batchSize) {
    for (const frame of frames.slice(i, i + batchSize)) {
      yield frame;
    }
    if (i + batchSize < frames.length) {
      await new Promise((resolve) => setTimeout(resolve, batchIntervalMs));
    }
  }
}

/** 安全触发回调：回调自身抛错不影响链路（与 Phase 1 reportError 同思路）。 */
function safeCall(fn, ...args) {
  try {
    fn?.(...args);
  } catch {
    // no-op
  }
}

/**
 * 创建 bridge 运行时（Phase 2；Phase 4 agent-consult 适配）。
 * 显式依赖 createStt/createPipeline/createOpenAiClient/createEdgeTts/createDecodePipeline；
 * 状态由 state-machine 纯函数管理。回调全部可选（index.js 透传 SDK req 回调）。
 * @param {Object} options
 * @param {Object} [options.providerConfig] - LLM 配置（baseUrl/apiKey/model）
 * @param {string} [options.modelPath] - sherpa 流式模型目录（默认插件 models/ 下）
 * @param {number} [options.inputSampleRate] - 输入 PCM 采样率（默认 24000）
 * @param {Array<object>} [options.tools] - OpenAI 兼容 function tools（agent-consult 场景注入）
 * @param {boolean} [options.autoRespondToAudio] - false=不自管回复（agent-consult 主路径，等 gateway 回流）；缺省/true=自管 LLM 回复
 * @param {number} [options.consultFlowbackTimeoutMs] - consult 回流等待超时（毫秒，默认 60000；测试注入短超时加速验证兜底路径）
 * @param {number} [options.playbackTailSuppressionMs] - 播放结束尾部静默抑制窗（毫秒，默认 300；方案 B 回声规避）
 * @param {boolean} [options.suppressPlaceholderReplies] - 占位句处理开关：true=中文等待语替换（默认）；false=完全静默（不播英文也不播等待语）
 * @param {Array<RegExp|string>} [options.placeholderPatterns] - 占位模式列表（默认 DEFAULT_PLACEHOLDER_PATTERNS）
 * @param {string} [options.placeholderReplacementText] - 中文等待语（默认「稍等，我查一下」）
 * @param {number} [options.placeholderDelayMs] - 等待语延迟触发阈值（默认 3000ms；consult ≤阈值不播，>阈值播）
 * @param {boolean} [options.classifyIntent] - LLM 意图分类开关（默认 true；false=不分类，回退延迟阈值）
 * @param {number} [options.classifyTimeoutMs] - 分类超时预算（默认 1500ms；超时降级播等待语）
 * @param {string} [options.intentClassifierPrompt] - 意图分类器提示词（默认三任务 JSON）
 * @param {string} [options.retryPromptText] - 转录不可理解时的重说提示文案（默认「抱歉没听清，请再说一次」）
 * @param {string} [options.readyText] - 连接就绪提示文案（默认「在的」）
 * @param {boolean} [options.ackEnabled] - 启动反馈「在的」开关（默认 true）
 * @param {string} [options.instructions] - agent 会话 instructions（透传 LLM system 提示，可选）
 * @param {(config: object) => object} [options.llmFactory] - LLM 工厂（默认 createOpenAiClient；测试注入 mock）
 * @param {(config: object) => object} [options.ttsFactory] - TTS 工厂（默认 createEdgeTts；测试注入 mock）
 * @param {(config: object) => object} [options.decoderFactory] - 解码器工厂（默认 createDecodePipeline；测试注入 mock）
 * @param {(text: string) => void} [options.onRecognizedText] - 识别出句回调
 * @param {(error: Error) => void} [options.onError] - 运行时错误回调
 * @param {(text: string) => void} [options.onUserMessage] - gateway 注入 user message 回调
 * @param {(audio: Buffer) => void} [options.onAudio] - 输出音频回调（20ms pcm16 帧）
 * @param {(reason?: string) => void} [options.onClearAudio] - 清音频回调（barge-in）
 * @param {(markName: string) => void} [options.onMark] - 输出 mark 回调
 * @param {(role: string, text: string, isFinal: boolean) => void} [options.onTranscript] - 转写回调
 * @param {(event: object) => void} [options.onEvent] - 事件回调
 * @param {(event: object) => void} [options.onToolCall] - tool call 回调
 * @param {() => void} [options.onReady] - 就绪回调
 * @param {() => void} [options.onClose] - 关闭回调
 * @returns {{
 *   supportsToolResultContinuation: boolean,
 *   connect: () => Promise<void>,
 *   sendAudio: (audio: Buffer) => void,
 *   setMediaTimestamp: (ts: number) => void,
 *   handleBargeIn: (options?: object) => void,
 *   submitToolResult: (callId: string, result: unknown, options?: object) => void,
 *   acknowledgeMark: () => void,
 *   close: () => void,
 *   isConnected: () => boolean,
 *   sendUserMessage: (text: string) => void,
 *   getState: () => string,
 * }}
 */
export function createBridgeRuntime({
  providerConfig,
  modelPath = DEFAULT_MODEL_PATH,
  inputSampleRate = DEFAULT_INPUT_SAMPLE_RATE,
  tools,
  autoRespondToAudio = true,
  consultFlowbackTimeoutMs = CONSULT_FLOWBACK_TIMEOUT_MS,
  playbackTailSuppressionMs = PLAYBACK_TAIL_SUPPRESSION_MS,
  suppressPlaceholderReplies = true,
  placeholderPatterns = DEFAULT_PLACEHOLDER_PATTERNS,
  placeholderReplacementText = DEFAULT_PLACEHOLDER_REPLACEMENT_TEXT,
  placeholderDelayMs = DEFAULT_PLACEHOLDER_DELAY_MS,
  classifyIntent = true,
  classifyTimeoutMs = DEFAULT_CLASSIFY_TIMEOUT_MS,
  intentClassifierPrompt = DEFAULT_INTENT_CLASSIFIER_PROMPT,
  retryPromptText = DEFAULT_RETRY_PROMPT_TEXT,
  readyText = DEFAULT_READY_TEXT,
  ackEnabled = true,
  instructions,
  llmFactory = createOpenAiClient,
  ttsFactory = createEdgeTts,
  decoderFactory = createDecodePipeline,
  onRecognizedText,
  onError,
  onUserMessage,
  onAudio,
  onClearAudio,
  onMark,
  onTranscript,
  onEvent,
  onToolCall,
  onReady,
  onClose,
}) {
  const llmConfig = {
    baseUrl: providerConfig?.baseUrl ?? DEFAULT_LLM_CONFIG.baseUrl,
    apiKey: resolveApiKey(providerConfig?.apiKey ?? DEFAULT_LLM_CONFIG.apiKey),
    model: providerConfig?.model ?? DEFAULT_LLM_CONFIG.model,
  };

  /** @type {import('../stt/sherpa-stt.js').Stt | null} */
  let stt = null;
  let pipeline = null;
  let llm = null;
  let tts = null;
  let decoder = null;
  let state = STATES.IDLE;
  let connected = false;
  let closed = false;

  // LLM/TTS 异步任务控制：递增 token 使旧任务失效（barge-in / 新消息打断）。
  let flowToken = 0;
  // 当前播放进度（playhead 契约）：已推 pcm16 帧数 × 20ms。
  let pushedFrames = 0;
  // mark 已确认标记（acknowledgeMark 由 Control UI 播放完成后回调）。
  let markAcknowledged = false;
  // sendAudio 输入审计（Phase 3 采样率实测证据）：累计收到 PCM 字节数。
  // e2e 验证：已知喂入音频时长 → inferredSampleRate = totalBytes / 2 / durationSeconds。
  let audioStats = { chunkCount: 0, totalBytes: 0 };
  // Phase 4 工具续接：LLM 返回 tool_calls 后保存待续接上下文（callId + 消息历史），
  // submitToolResult(callId, result) 回流后追加 tool 结果消息并重跑 LLM。
  let pendingToolCall = null;
  // Phase 4 consult 兜底定时器：autoRespondToAudio=false 出句后等待回流，超时触发自管 LLM 回复。
  let consultFallbackTimer = null;
  // 批次 A-6：该出句是否已由 consult 兜底回复过（防双回复）。生命周期：
  // 新 consult 启动（reportToGateway）重置 false；兜底回调先 sendUserMessage（false 不被拦）
  // 再置 true；真实 consult 回流到达时若为 true → 静默丢弃（兜底已回通用建议，不再重复回复）；
  // 用户打断/会话关闭（close）重置。
  let fallbackReplied = false;
  // 批次 A-7：兜底回复用 cleanedText（已清洗，防重复字），降级 rawText。
  // reportToGateway 保存本次 consult 的原始/清洗文本；兜底定时器回调优先用清洗文本
  //（「今天天天天天气」等重复字在 cleanedText 已被顺化，不再原样出），cleanedText 为空降级 rawText。
  let lastFallbackRawText = '';
  let lastFallbackReportText = '';
  // Phase 4 回声规避（方案 B）：播放锁。TTS 播放期间 + 播放结束后尾部静默窗内置 true，
  // audio-pipeline 据此跳过 STT feed（杜绝扬声器回声触发出句/consult 环）；
  // barge-in（用户真实说话）立即解锁恢复识别。
  let playbackLocked = false;
  // 播放结束尾部抑制窗定时器（到期后解锁）。
  let playbackUnlockTimer = null;
  // 中文等待语延迟触发定时器：consult working 提示注入后延迟 placeholderDelayMs 才播等待语；
  // 真实回复到达时取消（consult 快则不播）。幂等：重复 working 提示不重复启动。
  let placeholderTimer = null;
  // 方案 3.0 意图分类：出句后并行发起分类 LLM 调用，递增 token 取消在途分类任务
  //（consult 真实回复到达 / 新出句 / barge-in / close 时递增，防止旧分类结果误触发等待语）。
  let classifyToken = 0;
  // TTS 合成互斥 gate：edge-tts 单实例同一时刻只支持一个 synthesize（active 互斥抛错）。
  // 所有合成（runLlmTtsFlow / playDirectTts）经 withTtsGate 串行排队——前一个释放才允许下一个。
  // 优先级由 flowToken 机制保证：旧任务（等待语）拿到 gate 后 token 过期 → 跳过不合成；
  // 新任务（真实回复）排队在前 → 立即合成。gate 只保证不撞车，不阻塞抢占。
  let ttsGate = Promise.resolve();

  /**
   * TTS 合成互斥执行：排队等待前一个合成完成/失败，然后运行 fn。
   * 链不因单个任务失败而断（catch 后继续）。调用方在 fn 内自行做 token 检查。
   * @param {() => Promise<void>} fn
   * @returns {Promise<void>}
   */
  function withTtsGate(fn) {
    const run = ttsGate.then(fn, fn);
    ttsGate = run.then(
      () => undefined,
      () => undefined, // 吞掉错误，链继续（错误由 fn 内部自行处理）
    );
    return run;
  }

  // 提示语预合成缓存（澜影拍板）：固定文案预合成 pcm16 buffer，播放命中直接推帧
  //（零合成延迟/零 edge-tts 占用/从根上消除提示语与真实回复合成冲突）。惰性创建
  //（依赖 tts/decoder/gate），connect 时 warmup 默认文案。
  let presynthCache = null;

  /** 惰性创建预合成缓存（tts/decoder 工厂 + gate）。 */
  function getPresynthCache() {
    if (!presynthCache) {
      presynthCache = createPresynthCache({
        tts: getTts(),
        decoder: getDecoder(),
        gate: withTtsGate,
        providerSignature: {
          voice: 'zh-CN-XiaoxiaoNeural',
          baseUrl: llmConfig.baseUrl,
          model: llmConfig.model,
        },
        isCancelled: () => closed, // 批次 A-8 治本：close 后放弃在途预合成（防 free 后 decode 死循环）
      });
    }
    return presynthCache;
  }

  /** 上报运行时错误：回调自身抛错不影响链路。 */
  function reportError(error) {
    safeCall(onError, error);
  }

  /** 清理 consult 回流兜底定时器（回流到达 / barge-in / close / 新出句均需清理）。 */
  function clearConsultFallbackTimer() {
    if (consultFallbackTimer !== null) {
      clearTimeout(consultFallbackTimer);
      consultFallbackTimer = null;
    }
  }

  /** 清理播放锁尾部抑制窗定时器（新播放 / barge-in / close 均需清理）。 */
  function clearPlaybackUnlockTimer() {
    if (playbackUnlockTimer !== null) {
      clearTimeout(playbackUnlockTimer);
      playbackUnlockTimer = null;
    }
  }

  /** 播放结束：启动尾部静默抑制窗，到期后解锁恢复识别（方案 B）。 */
  function schedulePlaybackUnlock() {
    clearPlaybackUnlockTimer();
    playbackUnlockTimer = setTimeout(() => {
      playbackUnlockTimer = null;
      playbackLocked = false;
    }, playbackTailSuppressionMs);
  }

  /**
   * 播放结束清音频管道（串音防护）：放弃播放前未出句的残留音频（乱码/短语音段），
   * 防下一条语音拼接上一条转录。播放锁期间 pushPcm 不 feed → sherpa 收不到静音 →
   * 未出句的残留永不 endpoint → 播放结束后新语音拼接。
   */
  function resetAudioForPlaybackEnd() {
    try {
      pipeline?.resetStt();
    } catch (error) {
      reportError(error);
    }
  }

  /** 取消中文等待语延迟定时器（真实回复到达 / barge-in / close / 新占位提示前清理）。 */
  function clearPlaceholderTimer() {
    if (placeholderTimer !== null) {
      clearTimeout(placeholderTimer);
      placeholderTimer = null;
    }
  }

  /**
   * 延迟触发中文等待语（幂等）：consult working 提示注入后 placeholderDelayMs 仍无真实回复
   * → 播放等待语。真实回复到达会 clearPlaceholderTimer 取消；重复 working 提示幂等不重复播。
   */
  function schedulePlaceholderReplacement() {
    if (placeholderTimer !== null) return; // 幂等：已有定时器在跑不重复启动
    if (placeholderDelayMs <= 0) return; // 非正阈值：延迟逻辑关闭（保持立即播语义由调用方处理）
    placeholderTimer = setTimeout(() => {
      placeholderTimer = null;
      if (closed) return;
      if (state !== STATES.THINKING) {
        if (canTransition(state, EVENTS.RESET)) {
          state = transition(state, EVENTS.RESET);
        }
        if (canTransition(state, EVENTS.TEXT_INPUT)) {
          state = transition(state, EVENTS.TEXT_INPUT); // IDLE -> THINKING
        } else {
          return; // 无法进入 THINKING（如已 SPEAKING）：放弃本轮等待语
        }
      }
      flowToken += 1; // 取消在途旧任务（等待语优先）
      const token = flowToken;
      playDirectTts(placeholderReplacementText, token).catch((error) => {
        if (token === flowToken) reportError(error);
      });
    }, placeholderDelayMs);
  }

  /** 出句文本处理：Phase 1 记录 + 上送 gateway；Phase 2 转发 LLM 链路。 */
  function handleRecognizedText(text) {
    safeCall(onRecognizedText, text);
    // Phase 4 agent-consult：autoRespondToAudio=false 时 bridge 不自管回复，
    // 只上报 user final transcript（gateway 靠它触发强制 consult），
    // agent 结果会经 sendUserMessage 回流（回流的文本是 agent 结果 → LLM 转口语 → TTS）。
    if (autoRespondToAudio === false) {
      // 方案 3.0（串行增强）：先清洗+分类（单次 LLM 调用双输出 JSON），再上报 cleanedText——
      // main agent 收到通顺文本；needTool 决定是否播等待语。清洗失败降级用原始文本（不阻塞）。
      if (classifyIntent) {
        classifyCleanAndReport(text); // async fire-and-forget（内部完成上报 + 等待语 + 兜底定时器）
      } else {
        reportToGateway(text, text);
      }
      return;
    }
    sendUserMessage(text);
  }

  /**
   * 方案 3.0 意图分类 + 语音清洗（双输出，串行后上报）：
   * 单次 LLM 调用返回 JSON { needTool, cleanedText }。
   * - needTool=true → 播等待语；false → 不播
   * - cleanedText → 上报 gateway（main agent 输入顺化）；失败/超时 → 降级用原始文本上报
   * - 降级（失败/超时/解析失败）→ 播等待语（宁可多提示不可干等）
   * 真实回复到达 / 新出句 / barge-in / close 递增 classifyToken 取消。
   * @param {string} rawText
   */
  async function classifyCleanAndReport(rawText) {
    if (closed) return;
    const token = ++classifyToken;
    let result = null;
    // 方案 1 输入预处理：极端重复字先本地规约再送分类（缩短输入、减轻 LLM 负担、减少超时）
    const preprocessed = collapseRepeatedChars(rawText);
    try {
      const reply = await Promise.race([
        (async () => {
          let buf = '';
          for await (const chunk of getLlm().streamChat([
            { role: 'system', content: intentClassifierPrompt },
            { role: 'user', content: preprocessed },
          ])) {
            if (chunk.content) {
              buf += chunk.content;
              // 注意：不能 break 首 chunk——deepseek 流式首 chunk 往往不完整（如「{"」），
              // 只取首 chunk 会导致 parseClassifierJson 必然失败（degraded → 播等待语+上报原始）。
              // 必须累积全部 chunk 再解析（JSON 紧凑输出，流式分包但总量小，累积开销可忽略）。
            }
          }
          return buf;
        })(),
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error('intent classify timeout')), classifyTimeoutMs),
        ),
      ]);
      result = parseClassifierJson(reply);
    } catch (error) {
      result = null; // 超时/失败 → 降级
    }
    if (token !== classifyToken || closed) return; // 已被取消（真实回复/新出句/关闭）

    const degraded = result === null;
    const understandable = result?.understandable !== false; // 默认 true（宽松）
    const needTool = result?.needTool === true;
    const cleanedText = (result?.cleanedText ?? '').trim() || rawText;
    safeCall(onEvent, {
      direction: 'bridge',
      type: 'intent_classified',
      detail: `needTool=${needTool} degraded=${degraded} understandable=${understandable}: ${cleanedText.slice(0, 60)}`,
    });
    // 转录不可理解（明确乱码/纯噪音）：不发起 consult，播重说提示，状态回 LISTENING 等重说
    if (!understandable) {
      safeCall(onEvent, {
        direction: 'bridge',
        type: 'retry_prompt',
        detail: `unintelligible: ${rawText.slice(0, 60)}`,
      });
      flowToken += 1; // 取消在途旧任务（重说提示优先）
      const token = flowToken;
      playDirectTts(retryPromptText, token).catch((error) => {
        if (token === flowToken) reportError(error);
      });
      // 状态回 IDLE（重说提示播放完成后由 playDirectTts 处理 TTS_END→IDLE）；不发起 consult
      return;
    }
    // 串行：用清洗文本上报 gateway → 触发 consult（main agent 输入顺化）
    reportToGateway(rawText, cleanedText);
    if (needTool || degraded) {
      // 需要工具 → 播等待语；失败/超时降级 → 也播（宁可多提示不可干等）
      playWaitMessage();
    }
    // 不需要工具（needTool=false 且未降级）→ 不播，等 consult 真实回复
  }

  /**
   * 上报 gateway（user final transcript）+ 启动 consult 回流兜底定时器。
   * @param {string} rawText - 原始转录（cleanedText 为空时兜底降级用）
   * @param {string} reportText - 上报文本（清洗后优先，降级用原始）
   */
  function reportToGateway(rawText, reportText) {
    fallbackReplied = false; // 批次 A-6：新 consult 启动重置「已兜底」标志
    lastFallbackRawText = rawText; // 批次 A-7：保存原始文本（兜底降级用）
    lastFallbackReportText = reportText; // 批次 A-7：保存 cleanedText（兜底优先用）
    safeCall(onTranscript, 'user', reportText, true);
    // Phase 4 consult 兜底：启动回流等待定时器。60s 内回流到达（sendUserMessage 被调用）
    // → 清除定时器走正常回流路径；超时无回流（如 agent 只产 thinking 无 content → 前端 no text）
    // → bridge 自管 LLM 直接回复该出句，保证用户总能听到回复。
    // 批次 A-7：60s 覆盖正常工具 consult 30-60s（20s 兜底会抢答，真实答案被静默丢弃）。
    // barge-in / close / 新出句会取消旧定时器（clearConsultFallbackTimer）。
    clearConsultFallbackTimer();
    consultFallbackTimer = setTimeout(() => {
      consultFallbackTimer = null;
      if (closed) return; // 会话已关闭：不再兜底回复
      // 批次 A-7：兜底优先用 cleanedText（已清洗顺化，防重复字原样出），降级 rawText
      const fallbackText = (lastFallbackReportText || lastFallbackRawText || '').trim();
      if (!fallbackText) return;
      sendUserMessage(fallbackText); // 兜底先播（fallbackReplied 仍 false，不被拦）
      fallbackReplied = true; // 批次 A-6：然后置位——真实回流到达将被静默丢弃（防双回复）
    }, consultFlowbackTimeoutMs);
  }

  /**
   * 解析分类器 JSON 输出（容错：允许前后多余文本/代码块围栏；取首个 JSON 对象）。
   * @param {string} reply
   * @returns {{needTool: boolean, cleanedText: string, understandable: boolean} | null}
   */
  function parseClassifierJson(reply) {
    if (typeof reply !== 'string') return null;
    const start = reply.indexOf('{');
    const end = reply.lastIndexOf('}');
    if (start === -1 || end === -1 || end <= start) return null;
    try {
      const parsed = JSON.parse(reply.slice(start, end + 1));
      if (parsed && typeof parsed === 'object') {
        return {
          needTool: parsed.needTool === true,
          cleanedText: typeof parsed.cleanedText === 'string' ? parsed.cleanedText : '',
          understandable: parsed.understandable !== false, // 宽松：缺失默认 true
        };
      }
      return null;
    } catch (error) {
      return null;
    }
  }

  /** 播等待语（出句分类路径）：与 schedulePlaceholderReplacement 共用 playDirectTts 链路。 */
  function playWaitMessage() {
    if (closed) return;
    if (state !== STATES.THINKING) {
      if (canTransition(state, EVENTS.RESET)) {
        state = transition(state, EVENTS.RESET);
      }
      if (canTransition(state, EVENTS.TEXT_INPUT)) {
        state = transition(state, EVENTS.TEXT_INPUT); // IDLE -> THINKING
      } else {
        return; // 无法进入 THINKING（如已 SPEAKING）：放弃本轮等待语
      }
    }
    flowToken += 1; // 取消在途旧任务（等待语优先）
    const token = flowToken;
    // 批次 A-4 方案 B：播前静默窗——等用户尾音/扬声器回声衰减再出声，降低消费端
    // barge-in 检测把回声误判为打断的概率。延迟期间 consult 返回（flowToken 变）→ 取消
    // 等待语（真实回复直接到，不重复播）。
    setTimeout(() => {
      if (closed || token !== flowToken) return;
      playDirectTts(placeholderReplacementText, token).catch((error) => {
        if (token === flowToken) reportError(error);
      });
    }, PRESYNTH_PLAY_PRE_DELAY_MS);
  }

  /** pipeline 出句回调：LISTENING -> RECOGNIZING ->（有文本）THINKING -> LLM/TTS。 */
  function handleSpeechEnd() {
    if (!stt || closed) return;
    try {
      if (!canTransition(state, EVENTS.SPEECH_END)) return;
      state = transition(state, EVENTS.SPEECH_END); // LISTENING -> RECOGNIZING
      const result = stt.getResult();
      const text = (result?.text ?? '').trim();
      // 批次 B：单字噪音（非白名单）→ 丢弃，不上报/不 consult，回 IDLE 继续听
      if (isIgnorableUtterance(text)) {
        if (canTransition(state, EVENTS.RESET)) {
          state = transition(state, EVENTS.RESET);
        }
        return;
      }
      if (text) {
        if (canTransition(state, EVENTS.RECOGNIZED)) {
          state = transition(state, EVENTS.RECOGNIZED); // RECOGNIZING -> THINKING
        }
        // Phase 3 修复：有文本时保持 THINKING，不 RESET——runLlmTtsFlow（异步）完成后
        // TTS_END -> IDLE 由自身负责；此处 RESET 会抢跑把 THINKING 打回 IDLE，
        // 导致 canTransition(IDLE, TTS_START) 非法、SPEAKING 态永不进入
        // （e2e 暴露的 Phase 2 隐藏 bug：语音路径状态机断言从未被覆盖）。
        handleRecognizedText(text);
      } else if (canTransition(state, EVENTS.RESET)) {
        state = transition(state, EVENTS.RESET); // 无文本：回 IDLE 准备下一轮
      }
    } catch (error) {
      reportError(error);
    }
  }

  /** 惰性初始化 stt + pipeline（connect 或首次 sendAudio 触发，幂等）。 */
  function ensureInitialized() {
    if (closed) {
      throw new Error('TTS plugin bridge: closed, cannot initialize');
    }
    if (pipeline && stt) return;
    stt = createStt({ modelPath });
    // 方案 B 回声规避：把播放锁接进输入管道——锁定时 pushPcm 跳过 STT feed。
    pipeline = createPipeline({ stt, inputSampleRate, shouldSuppressInput: () => playbackLocked });
    pipeline.onSpeechEnd(handleSpeechEnd);
    pipeline.onError(reportError);
    connected = true;
  }

  /** 惰性创建 LLM 客户端（Phase 2；Phase 4 llmFactory 注入测试 mock）。 */
  function getLlm() {
    if (!llm) {
      llm = llmFactory(llmConfig);
    }
    return llm;
  }

  /** 惰性创建 edge-tts 封装（Phase 2；Phase 4 ttsFactory 注入测试 mock）。 */
  function getTts() {
    if (!tts) {
      tts = ttsFactory({ voice: 'zh-CN-XiaoxiaoNeural' });
    }
    return tts;
  }

  /** 惰性创建 mpg123 解码管线（Phase 2；Phase 4 decoderFactory 注入测试 mock）。 */
  function getDecoder() {
    if (!decoder) {
      decoder = decoderFactory();
    }
    return decoder;
  }

  /**
   * 文本入 → LLM → TTS → 解码 → onAudio 全链路（Phase 2 核心；Phase 4 工具续接）。
   * 异步执行：barge-in / 新消息通过 flowToken 递增取消旧任务。
   * 错误隔离：LLM/TTS 失败经 onError 上报，不崩溃进程；状态回退 IDLE。
   * 工具调用：LLM 返回 tool_calls 时上报 onToolCall 并保存待续接上下文
   * （submitToolResult 回流后续接同一消息历史），不直接 TTS。
   * @param {string} text - 当前 user 文本（续接场景传 '' 表示无新 user 消息，不上报 transcript）
   * @param {Array<{role: string, content: string | null, tool_calls?: Array}>} messages - LLM 消息数组
   * @param {number} token - 流程令牌（flowToken 快照，用于打断检测）
   */
  async function runLlmTtsFlow(text, messages, token) {
    try {
      // THINKING：LLM 调用（user transcript 仅在有新文本时上报；续接场景跳过）
      // Phase 4 防 consult 循环：真实用户出句已由 handleRecognizedText 的 false 分支上报过
      // user final；autoRespondToAudio=false 时这里的 text 是 gateway 回流（agent 结果），
      // 不再上报 user final，避免 gateway 把它误当成新语音触发强制 consult。
      if (text && autoRespondToAudio !== false) safeCall(onTranscript, 'user', text, true);
      let reply = '';
      let toolCalls = null;
      // Phase 4 防 consult 循环：agent-consult 主路径（autoRespondToAudio=false）的 consult
      // 由 gateway 强制管理，bridge LLM 只做口语转换，不传 tools——否则 bridge LLM 会主动调
      // consult 工具加剧循环。submitToolResult 续接能力保留（autoRespondToAudio=true 仍可用）。
      const llmTools = autoRespondToAudio === false ? undefined : (tools && tools.length > 0 ? tools : undefined);
      // 批次 A-7：bridge 自管 LLM 上下文前插 system prompt（运行时真实日期/口语化/顺化重复字/防编造）。
      // runLlmTtsFlow 只被 bridge 调用（兜底/autoRespondToAudio/回流口语转换）——不污染 gateway
      // consult 链路（forced consult 上下文由 OpenClaw 侧构建）。续接历史（pendingToolCall）不含
      // system 消息，每次 run 只前插一次，不会重复累积。
      const fullMessages = [
        { role: 'system', content: buildBridgeSystemPrompt() },
        ...messages,
      ];
      for await (const chunk of getLlm().streamChat(fullMessages, llmTools ? { tools: llmTools } : {})) {
        if (token !== flowToken) return; // 被打断：静默丢弃
        if (chunk.toolCalls && chunk.toolCalls.length > 0) {
          toolCalls = chunk.toolCalls;
          break; // 工具调用：不 TTS，等待 submitToolResult 续接
        }
        if (chunk.content) {
          reply += chunk.content;
          safeCall(onTranscript, 'assistant', reply, false);
        }
      }
      if (token !== flowToken) return;

      // 工具调用路径：上报 onToolCall + 保存续接上下文（assistant tool_call 消息已入历史）
      if (toolCalls) {
        for (const tc of toolCalls) {
          safeCall(onToolCall, {
            itemId: `tool-${tc.id}`,
            callId: tc.id,
            name: tc.name,
            args: tc.arguments,
          });
          pendingToolCall = {
            callId: tc.id,
            messages: [
              ...messages,
              {
                role: 'assistant',
                content: null,
                tool_calls: [
                  { id: tc.id, type: 'function', function: { name: tc.name, arguments: tc.arguments } },
                ],
              },
            ],
          };
        }
        return;
      }

      if (!reply.trim()) {
        throw new Error('LLM returned empty response');
      }
      // 过滤点 B（输出兜底）：LLM 回复命中 consult 占位模式。
      // suppressPlaceholderReplies=true → 改播中文等待语（替换 reply，不播英文占位）；
      // false → 完全静默（不播英文也不播等待语，状态回 IDLE）。
      if (isPlaceholderText(reply, placeholderPatterns)) {
        safeCall(onEvent, {
          direction: 'bridge',
          type: 'placeholder_suppressed',
          detail: reply.slice(0, 120),
        });
        if (!suppressPlaceholderReplies) {
          if (canTransition(state, EVENTS.RESET)) {
            state = transition(state, EVENTS.RESET); // THINKING -> IDLE，完全静默
          } else {
            state = STATES.IDLE;
          }
          return;
        }
        reply = placeholderReplacementText; // 中文等待语替换
      }
      safeCall(onTranscript, 'assistant', reply, true);
      if (token !== flowToken) return;

      // THINKING -> SPEAKING：TTS 合成 + 解码 + 推送
      if (canTransition(state, EVENTS.TTS_START)) {
        state = transition(state, EVENTS.TTS_START);
        // 方案 B：进入播放 → 置播放锁（audio-pipeline 抑制 STT 输入），防扬声器回声环
        clearPlaybackUnlockTimer();
        playbackLocked = true;
      }
      safeCall(onMark, 'response-start');
      pushedFrames = 0;
      markAcknowledged = false;

      // TTS 合成互斥（gate）：等待前一个合成释放后才开始本合成。await 后 token 过期（被
      // 新任务抢占/打断）→ 跳过不实际合成（不浪费 edge-tts WS 连接）。
      const synthToken = token;
      await withTtsGate(async () => {
        if (synthToken !== flowToken) return; // 拿到 gate 后发现被抢占 → 跳过
        const decodePipeline = getDecoder();
        await decodePipeline.reset(); // 复用解码器（第二段文本）
        const stream = getTts().synthesize(reply);
        for await (const frame of decodePipeline.decode(stream)) {
          if (synthToken !== flowToken) return; // 打断：丢弃未推帧
          safeCall(onAudio, frame.pcm16);
          pushedFrames += 1;
        }
      });
      if (token !== flowToken) return;

      // SPEAKING -> IDLE：TTS 完成
      if (canTransition(state, EVENTS.TTS_END)) {
        state = transition(state, EVENTS.TTS_END);
      }
      // 串音防护：播放结束清音频管道残留（乱码/短语音未出句段），防下条语音拼接
      resetAudioForPlaybackEnd();
      // 方案 B：播放结束 → 尾部静默抑制窗（300ms 防余音/回声尾巴误识别），到期后解锁
      schedulePlaybackUnlock();
      safeCall(onMark, 'response-end');
    } catch (error) {
      if (token !== flowToken) return; // 旧任务错误不报
      reportError(error);
      // 错误路径：回 IDLE 准备下一轮；播放锁立即释放（未播放/播放中断，无回声可抑制）
      clearPlaybackUnlockTimer();
      playbackLocked = false;
      if (canTransition(state, EVENTS.RESET)) {
        state = transition(state, EVENTS.RESET);
      } else {
        state = STATES.IDLE;
      }
    }
  }

  /**
   * 直接 TTS 播放（占位句中文等待语专用，过滤点 A）：不调 LLM，纯合成+解码+推送。
   * THINKING -> SPEAKING -> IDLE 完整走状态机，含播放锁与尾部抑制窗（方案 B 一致）。
   * @param {string} text - 待播放文本
   * @param {number} token - 流程令牌（flowToken 快照）
   */
  async function playDirectTts(text, token) {
    try {
      if (token !== flowToken) return;
      if (!text.trim()) return;
      // THINKING -> SPEAKING：直接 TTS 合成 + 解码 + 推送
      if (canTransition(state, EVENTS.TTS_START)) {
        state = transition(state, EVENTS.TTS_START);
        // 方案 B：进入播放 → 置播放锁（audio-pipeline 抑制 STT 输入），防扬声器回声环
        clearPlaybackUnlockTimer();
        playbackLocked = true;
      }
      safeCall(onMark, 'response-start');
      pushedFrames = 0;
      markAcknowledged = false;

      // 提示语预合成命中：直接推 pcm16 帧（零合成、零解码），不走 gate/实时合成
      const presynthPcm = getPresynthCache().get(text);
      if (presynthPcm) {
        const FRAME = 960; // 20ms @24k（FRAME_BYTES 同值）
        // 批次 A-4：presynth 推帧豁免 flowToken（方案 C）——消费端 barge-in 误判会经
        // gateway 反向 handleBargeIn → flowToken += 1 掐断在途推帧，导致等待语只播首字
        // /完全不播。等待语是提示语，应播完；真实打断由消费端本地 stopOutput 处理 +
        // 用户新语音触发新流程。只检查 closed（会话关闭则停）。
        for await (const frame of presynthFrames(presynthPcm, FRAME)) {
          if (closed) return; // 只检查会话关闭；豁免 flowToken（方案 C）
          safeCall(onAudio, frame);
          pushedFrames += 1;
        }
      } else {
        // miss → 实时合成（走 gate 互斥，与真实回复不撞车）
        const synthToken = token;
        await withTtsGate(async () => {
          if (synthToken !== flowToken) return; // 拿到 gate 后发现被抢占 → 跳过
          const decodePipeline = getDecoder();
          await decodePipeline.reset();
          const stream = getTts().synthesize(text);
          for await (const frame of decodePipeline.decode(stream)) {
            if (synthToken !== flowToken) return; // 打断：丢弃未推帧
            safeCall(onAudio, frame.pcm16);
            pushedFrames += 1;
          }
        });
      }
      if (closed) return; // 批次 A-4 方案 C：presynth 豁免后推帧完 flowToken 可能已变，仍收尾

      // SPEAKING -> IDLE：TTS 完成
      if (canTransition(state, EVENTS.TTS_END)) {
        state = transition(state, EVENTS.TTS_END);
      }
      // 串音防护：播放结束清音频管道残留（乱码/短语音未出句段），防下条语音拼接
      resetAudioForPlaybackEnd();
      // 方案 B：播放结束 → 尾部静默抑制窗（300ms 防余音/回声尾巴误识别），到期后解锁
      schedulePlaybackUnlock();
      safeCall(onMark, 'response-end');
    } catch (error) {
      if (token !== flowToken) return;
      reportError(error);
      clearPlaybackUnlockTimer();
      playbackLocked = false;
      if (canTransition(state, EVENTS.RESET)) {
        state = transition(state, EVENTS.RESET);
      } else {
        state = STATES.IDLE;
      }
    }
  }

  function sendAudio(audio) {
    if (closed) {
      reportError(new Error('TTS plugin bridge: sendAudio after close'));
      return;
    }
    if (!Buffer.isBuffer(audio)) {
      reportError(new TypeError('sendAudio: expects a Buffer (PCM16 LE)'));
      return;
    }
    // 输入审计：累计 PCM 字节（Phase 3 采样率实测证据，getAudioStats 暴露）
    audioStats.chunkCount += 1;
    audioStats.totalBytes += audio.length;
    try {
      ensureInitialized();
    } catch (error) {
      reportError(error);
      return;
    }
    // 先切 LISTENING 再喂音频：保证管道内同步回调（speechEnd）处于正确状态。
    if (canTransition(state, EVENTS.INPUT_START)) {
      state = transition(state, EVENTS.INPUT_START); // IDLE -> LISTENING
    }
    try {
      pipeline.pushPcm(audio);
    } catch (error) {
      reportError(error);
    }
  }

  function handleBargeIn(options) {
    // force consult 清场（gateway 强制 consult 机制，出句上报后 200ms 触发）：
    // 不是用户打断——只清浏览器缓冲（让浏览器停掉旧音频，准备 consult），
    // 不递增 flowToken/classifyToken（等待语要继续播完，不被误杀）、不做状态转换、
    // 不清 consult 兜底/等待语定时器、不解锁。真实用户打断走下方完整链路。
    if (options?.force === true) {
      safeCall(onClearAudio, 'barge-in');
      return;
    }
    // 管道保持不销毁：打断后继续听；VAD 静音计数随语音恢复自动清零。
    // Phase 2：递增 token 取消 LLM/TTS 异步任务 + 通知 gateway 清音频缓冲。
    flowToken += 1;
    clearConsultFallbackTimer(); // consult 兜底取消：用户打断，不再等待/触发兜底回复
    clearPlaceholderTimer(); // 等待语取消：用户打断，不再延迟播等待语
    classifyToken += 1; // 方案 3.0：用户打断 → 取消在途意图分类
    // 批次 C：barge-in 不立即解锁——用户打断时扬声器物理声音仍有输出延迟，
    // 锁解了声音还在 → 回声尾巴污染句首识别。播放中被打断时保持锁 + 追加
    // 短抑制窗（复用 schedulePlaybackUnlock，300ms）让回声先过去再恢复识别；
    // 非播放中打断（无回声）保持原语义直接解锁。
    clearPlaybackUnlockTimer();
    if (playbackLocked) {
      schedulePlaybackUnlock();
    } else {
      playbackLocked = false;
    }
    safeCall(onClearAudio, 'barge-in');
    if (canTransition(state, EVENTS.BARGE_IN)) {
      state = transition(state, EVENTS.BARGE_IN); // 任意活动态 -> LISTENING
    }
  }

  function close() {
    if (closed) return;
    closed = true;
    flowToken += 1; // 取消所有在途 LLM/TTS 任务
    clearConsultFallbackTimer(); // 会话关闭：取消 consult 兜底定时器（防关闭后触发）
    fallbackReplied = false; // 批次 A-6：会话关闭重置「已兜底」标志
    clearPlaceholderTimer(); // 会话关闭：取消等待语延迟定时器（防关闭后触发）
    classifyToken += 1; // 方案 3.0：会话关闭 → 取消在途意图分类（防关闭后误播等待语）
    clearPlaybackUnlockTimer(); // 方案 B：会话关闭取消尾部抑制窗定时器
    playbackLocked = false;
    try {
      pipeline?.destroy();
    } catch (error) {
      reportError(error);
    }
    pipeline = null;
    try {
      stt?.finalize();
    } catch (error) {
      reportError(error);
    }
    stt = null;
    try {
      decoder?.free(); // 释放 mpg123 WASM 内存
    } catch (error) {
      reportError(error);
    }
    decoder = null;
    llm = null;
    tts = null;
    connected = false;
    audioStats = { chunkCount: 0, totalBytes: 0 };
    if (canTransition(state, EVENTS.RESET)) {
      state = transition(state, EVENTS.RESET); // -> IDLE
    } else {
      state = STATES.IDLE; // 兜底（RESET 对所有状态合法，实际不可达）
    }
    safeCall(onClose, 'completed');
  }

  /**
   * 文本消息入口（SDK / 识别回调共用）。
   * IDLE --TEXT_INPUT--> THINKING（直接文本输入）；已在活动态则先重置再进 THINKING。
   * 触发 LLM→TTS 全链路（异步，fire-and-forget，错误经 onError）。
   * @param {string} text
   */
  function sendUserMessage(text) {
    if (typeof text !== 'string') {
      reportError(new TypeError('sendUserMessage: expects a string'));
      return;
    }
    const trimmed = text.trim();
    if (!trimmed) return; // 空文本忽略
    // 过滤点 A（源头）：gateway 注入的 consult 占位指令文本（buildForcedConsultCheckingPrompt
    // 英文「Briefly tell the person that you are checking with OpenClaw...」）。
    // suppressPlaceholderReplies=true → 延迟触发中文等待语（consult ≤placeholderDelayMs 不播，
    // 慢于阈值才播）；false → 完全静默（不播英文也不播等待语）。
    if (isPlaceholderText(trimmed, placeholderPatterns)) {
      safeCall(onEvent, {
        direction: 'bridge',
        type: 'placeholder_suppressed',
        detail: trimmed.slice(0, 120),
      });
      if (!suppressPlaceholderReplies) return; // 完全静默
      if (closed) return;
      // 等待语决策：
      // - classifyIntent=true（分类接管）：不在这里播——needTool 由 classifyCleanAndReport
      //   决定（needTool=true 播 / false 不播 / 降级播）。这里若再 schedulePlaceholderReplacement，
      //   定时器 3s 后独立触发 → 分类 needTool=false 时也播「稍等」（00:22 根因）。
      // - classifyIntent=false（无分类）：用延迟阈值机制（consult >阈值播，防焦虑）。
      if (!classifyIntent) {
        schedulePlaceholderReplacement();
      }
      return;
    }
    // 真实回复到达（非占位文本）：取消等待语延迟定时器 + 取消在途分类任务（consult 快则等待语不播）
    if (fallbackReplied) {
      // 批次 A-6：该出句已由 60s 兜底回复过 → 真实 consult 回流静默丢弃（防双回复；
      // 兜底已回通用建议，用户会再问，不再重复回复同一出句）
      clearPlaceholderTimer();
      classifyToken += 1;
      clearConsultFallbackTimer();
      return;
    }
    clearPlaceholderTimer();
    classifyToken += 1; // 方案 3.0：真实回复到达 → 取消在途意图分类（防旧分类误触发等待语）
    if (closed) {
      reportError(new Error('TTS plugin bridge: sendUserMessage after close'));
      return;
    }
    clearConsultFallbackTimer(); // consult 兜底取消：gateway 回流到达，正常走回流路径
    // 状态归位到 THINKING：IDLE 直接 TEXT_INPUT；活动态先 RESET 再 TEXT_INPUT；
    // 已在 THINKING（识别回调路径，handleSpeechEnd 已切）则保持。
    if (state !== STATES.THINKING) {
      if (canTransition(state, EVENTS.RESET)) {
        state = transition(state, EVENTS.RESET); // 任意态 -> IDLE
      }
      if (canTransition(state, EVENTS.TEXT_INPUT)) {
        state = transition(state, EVENTS.TEXT_INPUT); // IDLE -> THINKING
      } else {
        reportError(new Error(`sendUserMessage: cannot enter THINKING from ${state}`));
        return;
      }
    }
    flowToken += 1; // 取消在途旧任务
    const token = flowToken;
    // fire-and-forget：异步链路错误经 onError，不阻塞调用方
    runLlmTtsFlow(trimmed, [{ role: 'user', content: trimmed }], token).catch((error) => {
      if (token === flowToken) reportError(error);
    });
  }

  return {
    supportsToolResultContinuation: true,
    connect: async () => {
      try {
        ensureInitialized();
        // 提示语预合成预热（fire-and-forget，不阻塞 connect）：等待语 + 重说提示
        if (classifyIntent || suppressPlaceholderReplies) {
          try {
            getPresynthCache().warmup([
              placeholderReplacementText,
              retryPromptText,
              readyText,
            ]);
          } catch (error) {
            reportError(error);
          }
        }
        // ack 三层①：连接就绪播「在的」（系统就绪，请说话）。
        // 延迟一小段（等 warmup 预合成完成）再播。用当前 flowToken（不递增，不取消主链路）。
        if (ackEnabled && readyText.trim()) {
          setTimeout(() => {
            if (closed) return;
            if (state !== STATES.THINKING) {
              if (canTransition(state, EVENTS.RESET)) {
                state = transition(state, EVENTS.RESET);
              }
              if (canTransition(state, EVENTS.TEXT_INPUT)) {
                state = transition(state, EVENTS.TEXT_INPUT);
              } else {
                return;
              }
            }
            const token = flowToken; // 共享当前 flowToken（不递增）
            playDirectTts(readyText, token).catch((error) => {
              if (token === flowToken) reportError(error);
            });
          }, 300);
        }
        safeCall(onReady);
      } catch (error) {
        reportError(error);
        throw error;
      }
    },
    sendAudio,
    setMediaTimestamp: (ts) => {
      // Phase 2 填充：记录播放进度（毫秒）。Control UI playhead 对齐用。
      // 实际推进以已推 pcm16 帧数为准（pushedFrames × 20ms），ts 供外部对齐。
      if (typeof ts !== 'number' || !Number.isFinite(ts)) {
        reportError(new TypeError('setMediaTimestamp: expects a number (ms)'));
        return;
      }
      void ts; // Phase 2：进度以 pushedFrames 推算为主，ts 留待 Phase 3 端到端对齐
    },
    handleBargeIn,
    submitToolResult: (callId, result, options) => {
      // Phase 4 真正实现：LLM 发起工具调用（如 openclaw_agent_consult）后，
      // gateway 回流 tool result → 追加 tool 结果消息到历史 → 重跑 LLM 续接生成回复。
      // 无待续接上下文时（非工具场景回流）只记录事件，不静默吞掉 SDK 期待的结果。
      if (typeof callId !== 'string' || callId.length === 0) {
        reportError(new TypeError('submitToolResult: callId is required'));
        return;
      }
      if (!pendingToolCall || pendingToolCall.callId !== callId) {
        safeCall(onEvent, {
          direction: 'client',
          type: 'tool_result_submitted',
          detail: `${callId}: ${JSON.stringify(result ?? null).slice(0, 200)}`,
        });
        return;
      }
      if (options?.suppressResponse === true) {
        // 结果已由其他通道送达（如 agent 直接 speak），不续接新回复，清待续接上下文。
        pendingToolCall = null;
        safeCall(onEvent, {
          direction: 'client',
          type: 'tool_result_suppressed',
          detail: callId,
        });
        return;
      }
      const { messages } = pendingToolCall;
      pendingToolCall = null;
      const content = typeof result === 'string'
        ? result
        : (result && typeof result === 'object' ? JSON.stringify(result) : String(result ?? ''));
      const continued = [
        ...messages,
        { role: 'tool', tool_call_id: callId, content },
      ];
      flowToken += 1; // 取消在途旧任务（工具调用已结束）
      const token = flowToken;
      runLlmTtsFlow('', continued, token).catch((error) => {
        if (token === flowToken) reportError(error);
      });
    },
    acknowledgeMark: () => {
      // Phase 2 填充：Control UI 播放完成 mark 后回调；记录确认供 playhead 对齐。
      markAcknowledged = true;
    },
    close,
    isConnected: () => connected,
    sendUserMessage,
    getState: () => state,
    /**
     * sendAudio 输入审计（Phase 3 采样率实测证据）。
     * @returns {{chunkCount: number, totalBytes: number, inputSampleRate: number}}
     */
    getAudioStats: () => ({ ...audioStats, inputSampleRate }),
  };
}
