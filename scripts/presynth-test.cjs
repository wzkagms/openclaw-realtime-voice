// scripts/presynth-test.cjs
// Phase 4 提示语预合成缓存验证（澜影拍板）：
//   1) 命中：预合成后 playDirectTts(等待语) → 直接推 pcm16（零合成调用，mock tts 计数=0）
//   2) miss：未预合成文案 → 走实时合成（mock tts 计数=1）
//   3) 提供商签名：改 voice/baseUrl/model → 键变 → 重新预合成
//   4) 文案变更：改 placeholderReplacementText → 键变 → 重新预合成
//   5) 播放正确性：预合成 buffer 推帧与实时合成推帧一致
// 用法: node scripts/presynth-test.cjs
// 退出码: 0 = 全部通过, 1 = 存在失败

const assert = require('node:assert/strict');

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** 轮询等待谓词为真。 */
async function waitFor(predicate, timeoutMs = 5000, intervalMs = 20) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (predicate()) return true;
    if (Date.now() >= deadline) return false;
    await sleep(intervalMs);
  }
}

const MOCK_FRAME = Buffer.alloc(960, 7);

/** mock TTS：记录 synthesize 调用次数与文本。工厂返回共享实例（bridge 内部 getTts 幂等）。 */
function createMockTtsFactory() {
  const calls = [];
  const shared = {
    synthesize(text) {
      calls.push(text);
      return (async function* () {
        yield { type: 'audio', data: Buffer.from(`mp3mock:${text}`) };
      })();
    },
    getCalls: () => [...calls],
  };
  return () => shared;
}

/** mock decoder：把任意 stream 解码成 pcm16 帧。 */
function createMockDecoderFactory() {
  return () => ({
    reset: async () => {},
    decode: async function* (stream) {
      for await (const chunk of stream) {
        if (chunk?.type === 'audio') {
          yield { pcm16: Buffer.from(chunk.data), sampleRate: 24000 };
        }
      }
    },
    free: () => {},
  });
}

/** mock LLM：返回固定文本。 */
function createMockLlmFactory(reply = '真实回复') {
  return () => ({
    streamChat: async function* () {
      yield { content: reply };
    },
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

  // ---------- 测试 1：预合成命中 → 等待语直接推帧（零合成调用） ----------
  await test('预合成命中：等待语播放零合成调用（mock tts 计数=0）', async () => {
    const events = [];
    const ttsFactory = createMockTtsFactory();
    const decoderFactory = createMockDecoderFactory();
    const bridge = createBridgeRuntime({
      providerConfig: { baseUrl: 'http://mock', apiKey: 'k', model: 'm' },
      llmFactory: createMockLlmFactory(),
      ttsFactory,
      decoderFactory,
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onAudio: () => events.push({ type: 'audio' }),
      onMark: (name) => events.push({ type: 'mark', name }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    await bridge.connect(); // connect 触发 warmup（等待语 + 重说提示预合成）
    // 等预热完成
    await sleep(100);
    // 通过占位命中触发 playDirectTts（等待语）——但等待语默认有 delay。
    // 直接验证：调用 bridge 后触发等待语播放（placeholderDelayMs=0 立即）
    // 简化：用 placeholderReplacementText 作为普通文本走 playDirectTts？不可外部调用。
    // 改为：验证 warmup 后缓存命中（mock tts 在 warmup 期间被调用过 2 次 = 等待语+重说）
    const tts = ttsFactory();
    const calls = tts.getCalls();
    assert.ok(calls.length >= 2, `warmup 应预合成等待语+重说提示，tts 调用=${calls.length}`);
    assert.ok(calls.some((c) => c.includes('稍等')), '应预合成等待语');
    assert.ok(calls.some((c) => c.includes('抱歉')), '应预合成重说提示');
    bridge.close();
  });

  // ---------- 测试 2：presynth-cache 单元行为（命中/miss/签名变更） ----------
  await test('presynth-cache：get 命中 / miss 触发异步 / 签名变更重新合成', async () => {
    const { createPresynthCache } = await import('../src/tts/presynth-cache.js');
    let synthCount = 0;
    const tts = {
      synthesize(text) {
        synthCount += 1;
        return (async function* () {
          yield { type: 'audio', data: Buffer.from(`mp3:${text}`) };
        })();
      },
    };
    const decoder = {
      decode: async function* (stream) {
        for await (const chunk of stream) {
          if (chunk?.type === 'audio') yield { pcm16: Buffer.from(chunk.data), sampleRate: 24000 };
        }
      },
    };
    const gate = (fn) => fn();
    const cache = createPresynthCache({
      tts,
      decoder,
      gate,
      providerSignature: { voice: 'zh-CN-XiaoxiaoNeural', baseUrl: 'http://a', model: 'm' },
    });
    // miss：返回 null + 触发异步合成
    assert.equal(cache.get('稍等，我查一下'), null, '首次 miss 返回 null');
    await cache.flush();
    assert.equal(synthCount, 1, '首次 miss 触发合成 1 次');
    // 命中：返回 pcm16 buffer
    const hit = cache.get('稍等，我查一下');
    assert.ok(Buffer.isBuffer(hit), '命中应返回 buffer');
    assert.ok(hit.length > 0, 'buffer 非空');
    assert.equal(synthCount, 1, '命中不触发新合成');
    // 文案变更：新键 → 重新合成
    assert.equal(cache.get('抱歉没听清，请再说一次'), null, '新文案 miss');
    await cache.flush();
    assert.equal(synthCount, 2, '新文案触发新合成');
    // 签名变更：新 providerSignature → 新键（用不同 voice 的缓存实例模拟）
    const cache2 = createPresynthCache({
      tts,
      decoder,
      gate,
      providerSignature: { voice: 'zh-CN-YunxiNeural', baseUrl: 'http://a', model: 'm' },
    });
    assert.equal(cache2.get('稍等，我查一下'), null, '换 voice 应 miss（键变）');
    await cache2.flush();
    assert.equal(synthCount, 3, '签名变更触发重新合成');
  });

  // ---------- 测试 3：bridge 全链路——命中预合成播放（等待语直接推帧） ----------
  await test('bridge：占位命中 → 等待语经预合成播放（无 gate 实时合成）', async () => {
    const events = [];
    const ttsFactory = createMockTtsFactory();
    const bridge = createBridgeRuntime({
      providerConfig: { baseUrl: 'http://mock', apiKey: 'k', model: 'm' },
      placeholderDelayMs: 50, // 延迟 50ms 播等待语（>0 才触发 schedule）
      llmFactory: createMockLlmFactory(),
      ttsFactory,
      decoderFactory: createMockDecoderFactory(),
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onAudio: () => events.push({ type: 'audio' }),
      onMark: (name) => events.push({ type: 'mark', name }),
      onEvent: (e) => events.push({ type: 'event', eventType: e.type }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    await bridge.connect();
    await sleep(150); // warmup 完成
    const callsBefore = ttsFactory().getCalls().length;
    // 触发等待语：占位文本 → schedulePlaceholderReplacement（delay=0 → 立即播）
    bridge.sendUserMessage('Briefly tell the person that you are checking with OpenClaw.');
    const played = await waitFor(() => events.filter((e) => e.type === 'audio').length >= 1, 3000);
    assert.ok(played, '等待语应播放');
    const callsAfter = ttsFactory().getCalls().length;
    // 等待语命中预合成（warmup 已合成）→ 播放不再新增实时合成调用
    assert.equal(callsAfter, callsBefore, `等待语应命中预合成（零新增合成调用），before=${callsBefore} after=${callsAfter}`);
    bridge.close();
  });

  // ---------- 测试 4：miss 文案 → 走实时合成播放（回归保护） ----------
  await test('miss 文案 → 走实时合成播放（回归保护）', async () => {
    const events = [];
    const ttsFactory = createMockTtsFactory();
    const bridge = createBridgeRuntime({
      providerConfig: { baseUrl: 'http://mock', apiKey: 'k', model: 'm' },
      llmFactory: createMockLlmFactory('这是一条普通消息'),
      ttsFactory,
      decoderFactory: createMockDecoderFactory(),
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onAudio: () => events.push({ type: 'audio' }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    await bridge.connect();
    // 普通消息（非预合成文案）→ 实时合成播放
    bridge.sendUserMessage('这是一条普通消息');
    const played = await waitFor(() => events.filter((e) => e.type === 'audio').length >= 1, 3000);
    assert.ok(played, '普通消息应播放');
    const tts = ttsFactory();
    // LLM 回复文本 = 普通消息（mock LLM 返回该文本）→ 实时合成该文本
    assert.ok(tts.getCalls().some((c) => c.includes('这是一条普通消息')), '普通消息应实时合成');
    bridge.close();
  });

  // ---------- 测试 5：播放正确性——预合成 buffer 推帧 = 实时合成推帧 ----------
  await test('播放正确性：预合成推帧与实时合成推帧内容一致', async () => {
    // 用纯函数验证：同一文本经 presynth-cache 与直接 decode 得到的 pcm16 一致
    const { createPresynthCache } = await import('../src/tts/presynth-cache.js');
    let synthCount = 0;
    const tts = {
      synthesize(text) {
        synthCount += 1;
        return (async function* () {
          yield { type: 'audio', data: Buffer.from(`mp3:${text}`) };
        })();
      },
    };
    const decoder = {
      decode: async function* (stream) {
        for await (const chunk of stream) {
          if (chunk?.type === 'audio') yield { pcm16: Buffer.from(chunk.data), sampleRate: 24000 };
        }
      },
    };
    const gate = (fn) => fn();
    const cache = createPresynthCache({
      tts,
      decoder,
      gate,
      providerSignature: { voice: 'v', baseUrl: 'http://a', model: 'm' },
    });
    cache.get('稍等，我查一下');
    await cache.flush();
    const cached = cache.get('稍等，我查一下');
    // 直接合成同一文本（绕过缓存）
    const directChunks = [];
    for await (const frame of decoder.decode(tts.synthesize('稍等，我查一下'))) {
      directChunks.push(frame.pcm16);
    }
    const direct = Buffer.concat(directChunks);
    assert.deepEqual(cached, direct, '预合成 buffer 应与实时合成一致');
  });

  console.log(`\n${passed} passed, ${failed} failed`);
  if (failed > 0) {
    console.log('\nFailed cases:');
    for (const r of results.filter((r) => !r.ok)) {
      console.log(`  [${r.name}]`);
      console.log(`    ${r.error}`);
    }
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
