// src/tts/presynth-cache.js
// 提示语预合成缓存（澜影拍板）：常用提示语（等待语/重说提示等固定文案）预合成成
// pcm16 buffer，需要时直接播放——零合成延迟、零 edge-tts 实例占用（从根上消除
// 提示语与真实回复的合成冲突）、离线可用。
//
// 合成键 = hash(text + voice + rate + volume + pitch + baseUrl + model)——换 TTS
// 提供商/改文案自动失效重新预合成（澜影关注点）。
//
// 并发安全：预合成走 withTtsGate（与实时合成互斥，避免占用 edge-tts 时撞车）；
// 播放命中 buffer 不走 gate（纯内存推帧，无冲突）。

import { createHash } from 'node:crypto';

/** @typedef {Object} PresynthCacheConfig
 * @property {(text: string) => AsyncGenerator<{type: string, data?: Buffer}>} tts - edge-tts synthesize
 * @property {(stream: AsyncGenerator) => AsyncGenerator<{pcm16: Buffer}>} decoder - decode-pipeline decode
 * @property {(fn: () => Promise<void>) => Promise<void>} gate - TTS 合成互斥 gate（withTtsGate）
 * @property {object} providerSignature - 提供商签名 {voice, rate, volume, pitch, baseUrl, model}
 * @property {() => boolean} [isCancelled] - 可选取消回调（close 后返回 true → 放弃在途预合成）
 */

/** 计算合成键：provider 签名 + 文案 hash。 */
function buildCacheKey(text, providerSignature) {
  const sig = [
    providerSignature?.voice ?? '',
    providerSignature?.rate ?? '',
    providerSignature?.volume ?? '',
    providerSignature?.pitch ?? '',
    providerSignature?.baseUrl ?? '',
    providerSignature?.model ?? '',
  ].join('|');
  return createHash('sha1').update(`${sig}\n${text}`).digest('hex');
}

/**
 * 创建提示语预合成缓存。
 * @param {PresynthCacheConfig} config
 */
export function createPresynthCache({ tts, decoder, gate, providerSignature, isCancelled }) {
  if (typeof tts?.synthesize !== 'function') {
    throw new TypeError('createPresynthCache: tts.synthesize is required');
  }
  if (typeof decoder?.decode !== 'function') {
    throw new TypeError('createPresynthCache: decoder.decode is required');
  }
  if (typeof gate !== 'function') {
    throw new TypeError('createPresynthCache: gate is required (withTtsGate)');
  }
  const signature = { ...providerSignature };
  /** @type {Map<string, {key: string, pcm16: Buffer, pending: Promise<void> | null}>} */
  const cache = new Map();

  /** 合成 + 解码 → 整段 pcm16 buffer（经 gate 互斥）。 */
  async function synthesizeToBuffer(key, text) {
    await gate(async () => {
      if (cache.get(key)?.pcm16) return; // 已被并发预热填充
      if (isCancelled?.()) return; // 批次 A-8 治本：已取消（close 后）→ 放弃合成
      const pcmChunks = [];
      for await (const frame of decoder.decode(tts.synthesize(text))) {
        if (isCancelled?.()) return; // 合成中取消：放弃（edge-tts 流式中途可取消）
        pcmChunks.push(frame.pcm16);
      }
      const pcm16 = Buffer.concat(pcmChunks);
      if (isCancelled?.()) return; // 合成完成但已取消：丢弃缓存（不写入）
      const existing = cache.get(key);
      if (existing) {
        existing.pcm16 = pcm16;
        existing.pending = null;
      } else {
        cache.set(key, { key, pcm16, pending: null });
      }
    });
  }

  return {
    /**
     * 查询预合成缓存（同步）：命中返回 pcm16 Buffer；未命中触发异步预合成（不等待），
     * 返回 null（调用方走实时合成，本次不阻塞；下次命中）。
     * @param {string} text
     * @returns {Buffer | null}
     */
    get(text) {
      if (typeof text !== 'string' || text.trim().length === 0) return null;
      const key = buildCacheKey(text, signature);
      const entry = cache.get(key);
      if (entry?.pcm16) return entry.pcm16;
      // 未命中：触发异步预合成（幂等：同一 key 只合成一次）
      const existing = cache.get(key);
      if (!existing) {
        const entry = { key, pcm16: null, pending: null };
        cache.set(key, entry);
        entry.pending = synthesizeToBuffer(key, text).catch(() => {
          cache.delete(key); // 合成失败：清除占位，下次重新尝试
        });
      }
      return null;
    },

    /**
     * 预热（connect 时 fire-and-forget）：对固定文案预合成。
     * @param {string[]} texts
     */
    warmup(texts) {
      for (const text of texts) {
        if (typeof text === 'string' && text.trim().length > 0) {
          this.get(text); // get 触发异步预合成
        }
      }
    },

    /** 当前缓存条目数（测试/诊断）。 */
    size() {
      return cache.size;
    },

    /** 等待所有在途预合成完成（测试用）。 */
    async flush() {
      const pendings = Array.from(cache.values())
        .map((e) => e.pending)
        .filter(Boolean);
      await Promise.allSettled(pendings);
    },
  };
}
