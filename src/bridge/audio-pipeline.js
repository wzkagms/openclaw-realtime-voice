// src/bridge/audio-pipeline.js
// 输入音频管道：PCM16 LE Buffer → Float32 → LinearResampler(→16k) → 能量 VAD → sherpa feed。
// 方案 A 自研能量 VAD（零依赖，不下载 silero 模型）；sherpa endpoint 检测负责出句判定。
// 错误隔离：resampler / stt.feed 各阶段独立 try/catch，错误经 onError 上报，互不影响状态。

import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const { LinearResampler } = require('sherpa-onnx-node');

// sherpa 流式识别固定 16k（见 src/stt/sherpa-stt.js featConfig.sampleRate）。
export const TARGET_SAMPLE_RATE = 16000;

export const DEFAULT_VAD_CONFIG = Object.freeze({
  threshold: 0.01, // RMS 阈值：低于视为静音
  silenceWindowMs: 500, // 静音持续多久触发一次 endpoint 判定
});

/** @typedef {Object} VadConfig
 * @property {number} threshold - RMS 静音阈值
 * @property {number} silenceWindowMs - 静音窗口（毫秒）
 */

/** @typedef {Object} Stt
 * @property {(samples: Float32Array, sampleRate?: number) => void} feed
 * @property {() => boolean} isEndpoint
 * @property {() => {text: string}} getResult
 * @property {() => void} reset
 * @property {() => void} finalize
 */

/**
 * PCM16 LE Buffer → Float32Array（归一化 [-1, 1]）。
 * 奇数长度截断尾字节：实时音频流 chunk 边界可任意切。
 * @param {Buffer} buffer
 * @returns {Float32Array}
 */
export function pcm16ToFloat32(buffer) {
  const sampleCount = Math.floor(buffer.length / 2);
  const int16 = new Int16Array(buffer.buffer, buffer.byteOffset, sampleCount);
  const samples = new Float32Array(sampleCount);
  for (let i = 0; i < sampleCount; i++) {
    samples[i] = int16[i] / 32768;
  }
  return samples;
}

/**
 * 计算 RMS（root mean square）；空帧返回 0。
 * @param {Float32Array} samples
 * @returns {number}
 */
export function computeRms(samples) {
  if (samples.length === 0) return 0;
  let sumSquares = 0;
  for (let i = 0; i < samples.length; i++) {
    sumSquares += samples[i] * samples[i];
  }
  return Math.sqrt(sumSquares / samples.length);
}

/**
 * 自研能量 VAD（方案 A）：RMS 阈值判定语音/静音，静音窗口到期回调 onSilenceWindow。
 * 纯状态机，零依赖，不引入 sherpa Vad 类（避免 silero 模型下载）。
 * 静音窗口触发后清零静音计数——同一静音段内周期性（每窗口）触发，直到语音恢复或 reset。
 * @param {Object} options
 * @param {number} options.threshold - RMS 阈值
 * @param {number} options.silenceWindowMs - 静音窗口（毫秒）
 * @param {number} options.sampleRate - 输入采样率（窗口换算帧数）
 * @param {() => void} [options.onSpeechStart] - 进入语音段
 * @param {() => void} [options.onSilenceWindow] - 静音窗口到期（由调用方判定是否出句）
 */
export function createEnergyVad({
  threshold,
  silenceWindowMs,
  sampleRate,
  onSpeechStart,
  onSilenceWindow,
}) {
  const windowFrames = Math.round((silenceWindowMs / 1000) * sampleRate);
  let active = false;
  let silenceFrames = 0;

  return {
    /** @param {Float32Array} samples */
    process(samples) {
      if (computeRms(samples) >= threshold) {
        silenceFrames = 0;
        if (!active) {
          active = true;
          onSpeechStart?.();
        }
        return;
      }
      if (!active) return; // 未进入语音段前的静音不累积
      silenceFrames += samples.length;
      if (silenceFrames >= windowFrames) {
        silenceFrames = 0; // 周期性复查 endpoint，直到出句或语音恢复
        onSilenceWindow?.();
      }
    },
    isActive() {
      return active;
    },
    reset() {
      active = false;
      silenceFrames = 0;
    },
  };
}

/**
 * 创建输入音频管道（显式依赖：stt / inputSampleRate 必传，缺失即抛错）。
 * @param {Object} options
 * @param {Stt} options.stt - sherpa 流式封装（显式依赖）
 * @param {number} options.inputSampleRate - 输入采样率（如 48000 / 24000 / 16000）
 * @param {VadConfig} [options.vadConfig]
 * @param {() => boolean} [options.shouldSuppressInput] - 回声抑制回调（返回 true 时跳过本帧 VAD/feed；方案 B 播放锁）
 */
export function createPipeline({ stt, inputSampleRate, vadConfig = DEFAULT_VAD_CONFIG, shouldSuppressInput }) {
  if (!stt || typeof stt.feed !== 'function') {
    throw new TypeError('createPipeline: stt is required (sherpa stt wrapper with feed())');
  }
  if (typeof inputSampleRate !== 'number' || !Number.isFinite(inputSampleRate) || inputSampleRate <= 0) {
    throw new TypeError('createPipeline: inputSampleRate is required (positive number)');
  }

  const cfg = { ...DEFAULT_VAD_CONFIG, ...vadConfig };
  let resampler = new LinearResampler(inputSampleRate, TARGET_SAMPLE_RATE);
  /** @type {Stt | null} */
  let sttRef = stt;
  let destroyed = false;
  let speechStartCb = null;
  let speechEndCb = null;
  let errorCb = null;
  /** 回声抑制回调（方案 B 播放锁）：返回 true 时跳过 VAD/feed。 */
  let suppressInputFn = typeof shouldSuppressInput === 'function' ? shouldSuppressInput : null;

  const vad = createEnergyVad({
    threshold: cfg.threshold,
    silenceWindowMs: cfg.silenceWindowMs,
    sampleRate: TARGET_SAMPLE_RATE,
    onSpeechStart: () => speechStartCb?.(),
    onSilenceWindow: handleSilenceWindow,
  });

  /** 静音窗口到期：询问 sherpa 是否 endpoint，是则出句 + reset 准备下句。 */
  function handleSilenceWindow() {
    if (!sttRef) return;
    try {
      if (sttRef.isEndpoint()) {
        speechEndCb?.();
        sttRef.reset();
        vad.reset();
      }
    } catch (error) {
      reportError(error);
    }
  }

  /** 错误上报回调自身抛错时静默吞掉，绝不影响管道状态。 */
  function reportError(error) {
    try {
      errorCb?.(error);
    } catch {
      // no-op
    }
  }

  return {
    /**
     * 输入 PCM16 LE Buffer：逐样本转 Float32 → resample → VAD → feed。
     * 空帧（<2 字节）直接忽略；destroy 后调用抛清晰错误。
     * @param {Buffer} buffer
     */
    pushPcm(buffer) {
      if (destroyed) {
        throw new Error('createPipeline: pushPcm called after destroy()');
      }
      if (!Buffer.isBuffer(buffer)) {
        throw new TypeError('createPipeline: pushPcm expects a Buffer');
      }
      if (buffer.length < 2) return;
      // 方案 B 回声抑制：播放锁/尾部静默窗内跳过整段（不喂 STT、不污染 VAD 语音段计数）
      // —— 扬声器回声不进入识别，杜绝「回声→出句→consult」物理环；barge-in 由 gateway 侧
      // VAD 触发 handleBargeIn 解锁后自然恢复。
      // 播放前未出句的残留由 bridge 在播放结束（resetStt）时清，防下条语音拼接串音。
      if (suppressInputFn && suppressInputFn()) return;

      let resampled;
      try {
        resampled = resampler.resample(pcm16ToFloat32(buffer));
      } catch (error) {
        reportError(error);
        return; // 本帧跳过：resampler 崩不影响 stt / VAD 状态
      }

      vad.process(resampled);
      const activeStt = sttRef;
      try {
        if (activeStt && resampled.length > 0) {
          activeStt.feed(resampled, TARGET_SAMPLE_RATE);
        }
      } catch (error) {
        reportError(error); // feed 崩不影响已更新的 VAD 计数
      }
    },

    /** 注册句末回调（能量 VAD 静音窗口 + sherpa endpoint 确认后触发）。 */
    onSpeechEnd(cb) {
      speechEndCb = cb;
    },

    /** 注册语音开始回调（RMS 越过阈值进入语音段时触发，可选）。 */
    onSpeechStart(cb) {
      speechStartCb = cb;
    },

    /** 注册错误回调（管道各阶段异常统一上报，可选）。 */
    onError(cb) {
      errorCb = cb;
    },

    /** 设置回声抑制回调（方案 B 播放锁，可动态启停；传非函数即清除）。 */
    setShouldSuppressInput(cb) {
      suppressInputFn = typeof cb === 'function' ? cb : null;
    },

    /** 当前 VAD 是否处于语音段（供打断判断）。 */
    isSpeechActive() {
      return vad.isActive();
    },

    /**
     * 重置 STT 识别状态（播放结束/解锁时调用）：放弃播放前未出句的残留音频，
     * 防下一条语音拼接上一条转录（串音）。sherpa reset 清空当前段识别结果。
     */
    resetStt() {
      try {
        sttRef?.reset();
      } catch (error) {
        reportError(error);
      }
      vad.reset();
    },

    /** 释放资源：重置 resampler、清空 stt 引用、重置 VAD、清回调。 */
    destroy() {
      destroyed = true;
      resampler?.reset();
      resampler = null;
      vad.reset();
      sttRef = null;
      suppressInputFn = null;
      speechStartCb = null;
      speechEndCb = null;
      errorCb = null;
    },
  };
}
