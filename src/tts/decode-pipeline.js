// src/tts/decode-pipeline.js
// edge-tts mp3 流 → mpg123-decoder → PCM16 LE 20ms 帧。
// 路线 1（攒完整再解码）：mpg123 无 feed 流式接口，且 edge-tts chunk 边界未必对齐
// mp3 帧，逐 chunk 解码会报 MPG123_ERR —— 所以先 Buffer.concat 成完整 mp3 再整段 decode
// （见 external-context/mpg123-decoder/api-1.0.3.md 流式分块场景）。

import { MPEGDecoder } from 'mpg123-decoder';

// edge-tts-universal 契约：audio-24khz-48kbitrate-mono-mp3 → 24kHz / mono
// （见 external-context/edge-tts-universal/api-1.4.0.md 输出格式）
export const TARGET_SAMPLE_RATE = 24000;

// 20ms 帧大小：24000 Hz × 20ms = 480 samples × 2 bytes（Int16 LE）= 960 bytes
export const FRAME_BYTES = (TARGET_SAMPLE_RATE * 20 * 2) / 1000;

const PCM16_SCALE = 32767; // Float32(-1..1) → Int16 的缩放因子
const PCM16_MIN = -32768;
const PCM16_MAX = 32767;

/** @typedef {Object} DecodePipelineConfig
 * @property {boolean} [enableGapless] - 读 XING/Lame 头裁掉 delay/padding（默认 true）
 */

/** @typedef {Object} PcmFrame
 * @property {Buffer} pcm16 - 20ms PCM16 LE 帧（最后一帧可更短）
 * @property {number} sampleRate - 24000（edge-tts 契约，已校验）
 */

/** @typedef {Object} DecodePipeline
 * @property {(edgeTtsStream: AsyncGenerator) => AsyncGenerator<PcmFrame>} decode
 * @property {() => Promise<void>} reset
 * @property {() => void} free
 */

/**
 * 校验工厂入参：显式传入的 options 必须是纯对象。
 * 允许省略（enableGapless 有默认值），但 null / 数组 / 原始类型一律拒绝。
 * @param {unknown} options
 */
function validateOptions(options) {
  if (typeof options !== 'object' || options === null || Array.isArray(options)) {
    throw new TypeError('createDecodePipeline: options must be an object');
  }
}

/**
 * 校验 decode 入参：AsyncGenerator 或任何含 Symbol.asyncIterator 的对象。
 * @param {unknown} stream
 */
function assertAsyncIterable(stream) {
  if (!stream || typeof stream[Symbol.asyncIterator] !== 'function') {
    throw new TypeError('decode: edgeTtsStream must be an AsyncGenerator or async-iterable');
  }
}

/**
 * 收集 stream 中所有 type==='audio' 的 chunk.data → 完整 mp3 Buffer。
 * 无任何 audio chunk 视为空流，抛错避免解码出空 buffer 的假成功。
 * @param {AsyncGenerator} stream
 * @returns {Promise<Buffer>}
 */
async function collectAudioChunks(stream) {
  const chunks = [];
  for await (const chunk of stream) {
    if (chunk && chunk.type === 'audio' && Buffer.isBuffer(chunk.data)) {
      chunks.push(chunk.data);
    }
  }
  if (chunks.length === 0) {
    throw new Error('decode: edge-tts stream yielded no audio chunks');
  }
  return Buffer.concat(chunks);
}

/**
 * 校验解码结果契约：至少 1 声道 + 24kHz。
 * ⚠️ mpg123-decoder@1.0.3 硬编码输出 2 声道槽位（getDecodedAudioMultiChannel 第三参固定 2，
 *    见 src/MPEGDecoder.js），mono 源时两声道内容相同（复制）。
 *    因此不要求 channelData.length === 1，取左声道（channelData[0]）即完整 mono 数据。
 * @param {{channelData: Float32Array[], sampleRate: number}} result
 */
function validateDecodeResult({ channelData, sampleRate }) {
  const channelCount = Array.isArray(channelData) ? channelData.length : 'invalid';
  if (!(channelCount >= 1)) {
    throw new Error(`decode: expected at least 1 channel, got ${channelCount}`);
  }
  if (sampleRate !== TARGET_SAMPLE_RATE) {
    throw new Error(`decode: expected sampleRate ${TARGET_SAMPLE_RATE} (edge-tts contract), got ${sampleRate}`);
  }
}

/**
 * Float32Array（-1.0..1.0）→ Int16Array：乘 32767 + clamp [-32768, 32767]。
 * 纯函数，无副作用。
 * @param {Float32Array} samples
 * @returns {Int16Array}
 */
function float32ToInt16(samples) {
  const int16 = new Int16Array(samples.length);
  for (let i = 0; i < samples.length; i++) {
    const scaled = Math.round(samples[i] * PCM16_SCALE);
    int16[i] = scaled < PCM16_MIN ? PCM16_MIN : scaled > PCM16_MAX ? PCM16_MAX : scaled;
  }
  return int16;
}

/**
 * Int16Array → Buffer（LE）：小端平台内存即 LE，直接取 buffer 视图零拷贝。
 * @param {Int16Array} int16
 * @returns {Buffer}
 */
function int16ToBuffer(int16) {
  return Buffer.from(int16.buffer, int16.byteOffset, int16.byteLength);
}

/**
 * 按 20ms 帧切分 PCM16 Buffer；最后一帧可更短；空 buffer 天然零 yield。
 * subarray 零拷贝（共享内存），仍满足 {pcm16: Buffer} 契约。
 * @param {Buffer} pcm16
 * @returns {Generator<Buffer>}
 */
function* chunkFrames(pcm16) {
  for (let offset = 0; offset < pcm16.length; offset += FRAME_BYTES) {
    const frame = pcm16.subarray(offset, offset + FRAME_BYTES);
    if (frame.length > 0) {
      yield frame;
    }
  }
}

/**
 * 创建 mpg123 解码管线：edge-tts AsyncGenerator → PCM16 LE 20ms 帧 AsyncGenerator。
 * 解码器惰性初始化（首次 decode 时 new + await ready）；reset() 复用实例解码第二段；
 * free() 释放 WASM 内存，实例作废，下次 decode 自动重建。
 * @param {DecodePipelineConfig} [options]
 * @returns {DecodePipeline}
 */
export function createDecodePipeline(options = {}) {
  validateOptions(options);
  const { enableGapless = true } = options;

  /** @type {Promise<import('mpg123-decoder').MPEGDecoder> | null} */
  let decoderPromise = null;
  /** @type {import('mpg123-decoder').MPEGDecoder | null} */
  let decoderInstance = null;

  /** 惰性初始化：首次调用时创建实例并等 WASM ready；并发调用共享同一 promise。 */
  async function getDecoder() {
    if (!decoderPromise) {
      decoderPromise = (async () => {
        const instance = new MPEGDecoder({ enableGapless });
        await instance.ready;
        decoderInstance = instance;
        return instance;
      })();
    }
    return decoderPromise;
  }

  return {
    async *decode(edgeTtsStream) {
      assertAsyncIterable(edgeTtsStream);
      const decoder = await getDecoder();
      const fullMp3 = await collectAudioChunks(edgeTtsStream);
      // 批次 A-8 治标：close() 会 free decoder，在途合成（collectAudioChunks 是 await 点）
      // 期间若 decoder 已被 free（decoderInstance 置 null），仍用旧实例 decode 会死循环
      //（mpg123 读悬空 WASM 指针返回 -1 不推进 → 无限 console.error 阻塞事件循环）。
      // 这里校验：decoder !== decoderInstance → 抛错一次性结束（不进入死循环）。
      if (decoder !== decoderInstance) {
        throw new Error('decode-pipeline: decoder was freed during synthesis, aborting decode');
      }
      const result = decoder.decode(fullMp3);
      // 解码错误不中断：mpg123 会跳过坏帧继续，可能有 gap 但音频仍在
      if (result.errors && result.errors.length > 0) {
        console.warn(`decode-pipeline: ${result.errors.length} decode error(s), continuing`, result.errors);
      }
      validateDecodeResult(result);
      // 取左声道为 mono：mpg123-decoder 硬编码双声道槽位，mono 源两声道相同（复制）
      const pcm16 = int16ToBuffer(float32ToInt16(result.channelData[0]));
      for (const frame of chunkFrames(pcm16)) {
        yield { pcm16: frame, sampleRate: result.sampleRate };
      }
    },

    /** 复位解码器状态，可复用同一实例解码下一段。 */
    async reset() {
      if (decoderPromise) {
        const instance = await decoderPromise;
        await instance.reset();
      }
    },

    /** 释放 WASM 内存；未初始化（从未 decode）时 no-op。 */
    free() {
      if (decoderInstance) {
        decoderInstance.free();
        decoderInstance = null;
        decoderPromise = null;
      }
    },
  };
}
