// src/stt/sherpa-stt.js
// sherpa-onnx-node 流式识别封装：16k PCM Float32 入 → 文本出（endpoint 出句）。
// 关键约束（1.13.5）：OnlineStream/OnlineRecognizer 无 free 方法（streaming-test.cjs 报错根因）；
// 出句后用 recognizer.reset(stream) 复用 stream 清状态；finalize 只清引用不释放句柄。
//
// Phase 4 预加载（澜影拍板，实测每次会话重建 ~10s）：模块级缓存 OnlineRecognizer 单例
// （int8 ~190MB 常驻），会话间复用 recognizer，每次会话仅 createStream（~1ms）——
// 消除每次 bridge 重建的模型加载 + onnxruntime session 初始化 + 首推预热开销。
// 并发安全：sherpa OnlineRecognizer 支持多 stream 并发（每会话独立 stream）。
// 模型变更/配置差异走不同缓存键；模型文件更新需 gateway 重启（缓存单例跨会话常驻）。

import { createRequire } from 'node:module';
import { existsSync } from 'node:fs';
import { join } from 'node:path';

const require = createRequire(import.meta.url);
const { OnlineRecognizer } = require('sherpa-onnx-node');

/** @typedef {Object} SttConfig
 * @property {string} modelPath - sherpa streaming 模型目录（显式依赖，不猜环境）
 * @property {{sampleRate?: number, featureDim?: number}} [featConfig]
 * @property {{rule1MinTrailingSilence?: number, rule2MinTrailingSilence?: number, rule3MinUtteranceLength?: number}} [endpointConfig]
 */

/** @typedef {Object} Stt
 * @property {(samples: Float32Array, sampleRate?: number) => void} feed
 * @property {() => boolean} isEndpoint
 * @property {() => {text: string}} getResult
 * @property {() => void} reset
 * @property {() => void} finalize
 */

const DEFAULT_FEAT_CONFIG = Object.freeze({ sampleRate: 16000, featureDim: 80 });
const DEFAULT_ENDPOINT_CONFIG = Object.freeze({
  rule1MinTrailingSilence: 2.4,
  rule2MinTrailingSilence: 1.2,
  rule3MinUtteranceLength: 20,
});

// 与已验证 scripts/streaming-test.cjs 相同的模型目录布局
const ENCODER_FILE = 'encoder-epoch-99-avg-1.int8.onnx';
const DECODER_FILE = 'decoder-epoch-99-avg-1.int8.onnx';
const JOINER_FILE = 'joiner-epoch-99-avg-1.int8.onnx';
const TOKENS_FILE = 'tokens.txt';
const MODEL_FILES = [ENCODER_FILE, DECODER_FILE, JOINER_FILE, TOKENS_FILE];

/**
 * 校验工厂入参（边界校验 + 显式依赖 modelPath）。
 * @param {SttConfig} config
 */
function validateConfig(config) {
  if (!config || typeof config !== 'object') {
    throw new TypeError('createStt: config object is required');
  }
  const { modelPath } = config;
  if (typeof modelPath !== 'string' || modelPath.length === 0) {
    throw new TypeError('createStt: modelPath is required (path to sherpa streaming model dir)');
  }
  const missing = MODEL_FILES.filter((file) => !existsSync(join(modelPath, file)));
  if (missing.length > 0) {
    throw new Error(`createStt: model files missing in ${modelPath}: ${missing.join(', ')}`);
  }
}

/**
 * 组装 OnlineRecognizer 构造配置（仿 streaming-test.cjs 已验证参数）。
 * @param {string} modelPath
 * @param {{sampleRate: number, featureDim: number}} featConfig
 * @param {object} endpointConfig
 */
function buildRecognizerConfig(modelPath, featConfig, endpointConfig) {
  return {
    featConfig,
    modelConfig: {
      transducer: {
        encoder: join(modelPath, ENCODER_FILE),
        decoder: join(modelPath, DECODER_FILE),
        joiner: join(modelPath, JOINER_FILE),
      },
      tokens: join(modelPath, TOKENS_FILE),
      numThreads: 2,
      provider: 'cpu',
      debug: 0,
    },
    enableEndpointDetection: true,
    enableEndpoint: true,
    ...endpointConfig,
  };
}

/**
 * feed 入参校验（Float32Array + sampleRate 匹配 + 非空）。
 * @param {unknown} samples
 * @param {number} sampleRate
 * @param {{sampleRate: number}} featConfig
 */
function validateSamples(samples, sampleRate, featConfig) {
  if (!(samples instanceof Float32Array)) {
    throw new TypeError('feed: samples must be a Float32Array');
  }
  if (samples.length === 0) {
    throw new RangeError('feed: samples must not be empty');
  }
  if (sampleRate !== featConfig.sampleRate) {
    throw new RangeError(
      `feed: sampleRate ${sampleRate} does not match recognizer sampleRate ${featConfig.sampleRate}`,
    );
  }
}

/**
 * 一次性 feed + decode（acceptWaveform → while(isReady) decode）。
 * @param {import('sherpa-onnx-node').OnlineStream} stream
 * @param {import('sherpa-onnx-node').OnlineRecognizer} recognizer
 * @param {{sampleRate: number, featureDim: number}} featConfig
 * @param {Float32Array} samples
 * @param {number} sampleRate
 */
function feedSamples(stream, recognizer, featConfig, samples, sampleRate) {
  validateSamples(samples, sampleRate, featConfig);
  stream.acceptWaveform({ sampleRate, samples });
  while (recognizer.isReady(stream)) {
    recognizer.decode(stream);
  }
}

/** finalize 后所有方法都应抛错，防止对空引用操作。 */
function assertActive(recognizer, stream) {
  if (!recognizer || !stream) {
    throw new Error('createStt: instance has been finalized, create a new one');
  }
}

// Phase 4 预加载缓存：modelPath + feat + endpoint 配置组合 → 共享 recognizer 单例。
// 键 = 序列化的配置签名（模型目录 + 特征/端点参数），不同配置独立实例。
// recognizer 单例常驻（refCount 跟踪使用方；finalize 只减计数不清缓存——
// 模型变更需 gateway 重启，与「模型变更走重启」风险约定一致）。
const recognizerCache = new Map();

/** 计算缓存键（modelPath + 展开后的 feat/endpoint 配置）。 */
function buildCacheKey(modelPath, featConfig, endpointConfig) {
  return JSON.stringify([modelPath, featConfig, endpointConfig]);
}

/** 获取共享 recognizer 单例：缓存命中直接复用，未命中则创建并计数。 */
function getSharedRecognizer(modelPath, featConfig, endpointConfig) {
  const key = buildCacheKey(modelPath, featConfig, endpointConfig);
  const cached = recognizerCache.get(key);
  if (cached) {
    cached.refCount += 1;
    return cached;
  }
  const recognizer = new OnlineRecognizer(buildRecognizerConfig(modelPath, featConfig, endpointConfig));
  const entry = { recognizer, refCount: 1 };
  recognizerCache.set(key, entry);
  return entry;
}

/**
 * 创建 sherpa 流式识别器封装。
 * @param {SttConfig} config
 * @returns {Stt}
 */
export function createStt(config) {
  validateConfig(config);
  const { modelPath } = config;
  const featConfig = { ...DEFAULT_FEAT_CONFIG, ...config.featConfig };
  const endpointConfig = { ...DEFAULT_ENDPOINT_CONFIG, ...config.endpointConfig };

  // Phase 4 预加载：复用共享 recognizer 单例，每次会话仅 createStream（轻量）。
  const entry = getSharedRecognizer(modelPath, featConfig, endpointConfig);
  let recognizer = entry.recognizer;
  let stream = recognizer.createStream();
  let finalized = false;

  return {
    feed(samples, sampleRate = featConfig.sampleRate) {
      assertActive(recognizer, stream);
      feedSamples(stream, recognizer, featConfig, samples, sampleRate);
    },
    isEndpoint() {
      assertActive(recognizer, stream);
      return recognizer.isEndpoint(stream);
    },
    getResult() {
      assertActive(recognizer, stream);
      return recognizer.getResult(stream);
    },
    reset() {
      assertActive(recognizer, stream);
      recognizer.reset(stream);
    },
    finalize() {
      if (finalized) return;
      finalized = true;
      // 只释放本会话 stream 引用；recognizer 单例保留（其他会话/下个会话继续复用）。
      recognizer = null;
      stream = null;
      entry.refCount -= 1;
    },
  };
}

/**
 * 预加载缓存观测（测试/诊断用）：返回当前缓存条目快照。
 * @returns {Array<{key: string, refCount: number}>}
 */
export function getRecognizerCacheSnapshot() {
  return Array.from(recognizerCache.entries()).map(([key, entry]) => ({
    key,
    refCount: entry.refCount,
  }));
}
