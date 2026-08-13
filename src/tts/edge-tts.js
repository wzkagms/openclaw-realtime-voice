// src/tts/edge-tts.js
// edge-tts-universal@1.4.0 流式合成封装：text → mp3 audio chunk + WordBoundary/SentenceBoundary 事件。
// 关键约束（1.4.0）：stream() 是 AsyncGenerator，WS 连接发生在迭代过程中（边 yield 边连）；
// 每个 Communicate 实例只能 stream() 一次 → 重试必须 new Communicate 重新迭代。
// WordBoundary 时间单位是 100 纳秒，原样透传不改（除以 10000 才是毫秒）。

import { Communicate } from 'edge-tts-universal';

/** @typedef {Object} EdgeTtsConfig
 * @property {string} [voice] - 语音短名/全名，默认 'zh-CN-XiaoxiaoNeural'
 * @property {string} [rate] - 语速，必须带符号，如 '+20%'
 * @property {string} [volume] - 音量，必须带符号，如 '+10%'
 * @property {string} [pitch] - 音调，Hz 必须带符号，如 '+5Hz'
 * @property {string} [proxy] - 代理 URL
 * @property {number} [connectionTimeout] - WS 连接超时 ms
 * @property {number} [maxRetries] - 网络失败重试次数，默认 2，指数退避 500ms → 1000ms
 */

/** @typedef {Object} EdgeTtsChunk
 * @property {'audio'|'WordBoundary'|'SentenceBoundary'} type
 * @property {Buffer} [data] - audio 类型：mp3 二进制
 * @property {string} [text] - WordBoundary 词文本
 * @property {number} [duration] - 时长，单位 100 纳秒（原样透传）
 * @property {number} [offset] - 流起始偏移，单位 100 纳秒（原样透传）
 */

/** @typedef {Object} EdgeTts
 * @property {(text: string) => AsyncGenerator<EdgeTtsChunk>} synthesize
 */

const DEFAULT_VOICE = 'zh-CN-XiaoxiaoNeural';
const DEFAULT_MAX_RETRIES = 2;
const BACKOFF_BASE_MS = 500;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** 指数退避：第 n 次重试等待 500 * 2^n ms（500 → 1000）。 */
const computeBackoffMs = (retryIndex) => BACKOFF_BASE_MS * 2 ** retryIndex;

/**
 * 校验工厂入参（边界校验；所有字段可选，但类型必须正确）。
 * @param {EdgeTtsConfig} config
 */
function validateConfig(config) {
  if (config === null || typeof config !== 'object') {
    throw new TypeError('createEdgeTts: config must be an object');
  }
  const stringFields = ['voice', 'rate', 'volume', 'pitch', 'proxy'];
  for (const name of stringFields) {
    if (config[name] !== undefined && typeof config[name] !== 'string') {
      throw new TypeError(`createEdgeTts: ${name} must be a string`);
    }
  }
  if (
    config.connectionTimeout !== undefined &&
    (typeof config.connectionTimeout !== 'number' ||
      !Number.isFinite(config.connectionTimeout) ||
      config.connectionTimeout <= 0)
  ) {
    throw new TypeError('createEdgeTts: connectionTimeout must be a positive number (ms)');
  }
  if (config.maxRetries !== undefined && (!Number.isInteger(config.maxRetries) || config.maxRetries < 0)) {
    throw new TypeError('createEdgeTts: maxRetries must be a non-negative integer');
  }
}

/** synthesize 入参校验（text 必须非空字符串）。 */
function validateText(text) {
  if (typeof text !== 'string' || text.trim().length === 0) {
    throw new TypeError('synthesize: text must be a non-empty string');
  }
}

/** 组装 Communicate 构造 options：voice 必填（默认晓晓），其余仅透传已配置项。 */
function buildCommunicateOptions(config) {
  const options = { voice: config.voice ?? DEFAULT_VOICE };
  for (const key of ['rate', 'volume', 'pitch', 'proxy', 'connectionTimeout']) {
    if (config[key] !== undefined) {
      options[key] = config[key];
    }
  }
  return options;
}

/** 全部重试耗尽后的失败错误（含原始信息 + cause 链）。 */
function makeExhaustedError(lastError, attempts) {
  return new Error(`createEdgeTts.synthesize: failed after ${attempts} attempt(s): ${lastError.message}`, {
    cause: lastError,
  });
}

/** 已产出音频后失败：重试会产生重复音频，直接抛出（由上层 fallback 链接管）。 */
function makeAudioDeliveredError(error) {
  return new Error(
    `createEdgeTts.synthesize: stream failed after audio was delivered, refusing to retry (would duplicate audio): ${error.message}`,
    { cause: error },
  );
}

/**
 * 带重试的流式合成（generator 感知：每轮 attempt 新建 Communicate，stream() 每实例仅一次）。
 * 失败发生在迭代过程中时，旧实例已不可复用，必须 new Communicate 再迭代。
 * 已产出 audio 后失败不重试——重试会从文本开头重新合成，导致重复音频。
 * @param {string} text
 * @param {object} options - Communicate options（含 voice）
 * @param {number} maxRetries
 * @param {typeof Communicate} CommunicateImpl
 * @returns {AsyncGenerator<EdgeTtsChunk>}
 */
async function* streamWithRetry(text, options, maxRetries, CommunicateImpl) {
  let lastError = null;
  for (let attempt = 0; ; attempt++) {
    const communicate = new CommunicateImpl(text, options);
    let yieldedAudio = false;
    try {
      for await (const chunk of communicate.stream()) {
        if (chunk.type === 'audio' && chunk.data) {
          yieldedAudio = true;
        }
        yield chunk; // 原样透传：WordBoundary 100 纳秒单位不改
      }
      return;
    } catch (error) {
      lastError = error;
      if (yieldedAudio) {
        throw makeAudioDeliveredError(error);
      }
      if (attempt >= maxRetries) {
        throw makeExhaustedError(lastError, attempt + 1);
      }
      await sleep(computeBackoffMs(attempt));
    }
  }
}

/**
 * 创建 edge-tts 流式合成封装。
 * @param {EdgeTtsConfig} [config]
 * @param {{Communicate?: typeof Communicate}} [deps] - 可选依赖注入（测试用）
 * @returns {EdgeTts}
 */
export function createEdgeTts(config = {}, deps = { Communicate }) {
  validateConfig(config);
  const { Communicate: CommunicateImpl } = deps;
  if (typeof CommunicateImpl !== 'function') {
    throw new TypeError('createEdgeTts: deps.Communicate must be a constructor');
  }
  const maxRetries = config.maxRetries ?? DEFAULT_MAX_RETRIES;
  const options = buildCommunicateOptions(config);
  let active = false;

  return {
    synthesize(text) {
      validateText(text);
      if (active) {
        throw new Error(
          'createEdgeTts: synthesize() is already in progress; each instance supports one active stream',
        );
      }
      active = true;
      return (async function* () {
        try {
          yield* streamWithRetry(text, options, maxRetries, CommunicateImpl);
        } finally {
          active = false;
        }
      })();
    },
  };
}
