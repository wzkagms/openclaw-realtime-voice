// scripts/presynth-pacing-test.cjs
// 批次 A-4 推帧节奏验证（A 分批推帧 + B 播前静默窗 + C 豁免 flowToken）+ 批次 A-6 批间收缩：
//   presynth buffer 推帧 async generator 分 3-4 批、批间 50ms（A-6 由 300ms 收缩——消除
//   分批暂停造成的播放断流卡顿，同时保留分批推帧让消费端「每批 audio 到达重置 barge-in
//   恢复标志」）；批内帧微任务连推。
// 演进背景（根因证据链）：
//   - 原版：同步 for 循环（108 帧 µs 级推完，无让出）→ 消费端只处理首帧（只播「稍」）
//   - 批次 A：setImmediate 让步 → Node 空载下近乎同步（108 帧 0.5ms 推完）→ 仍只播首字
//   - 批次 A-2：setTimeout(20) 节流（108 帧 ~2.14s 慢推）→ 消费端丢弃整个片段（完全不播）
//   - 批次 A-3：async generator + for await 微任务推帧（毫秒级）→ 仍只播首字
//     （消费端 barge-in 误判回声 → 全量 stop；瞬间推完无后续帧恢复）
//   - 批次 A-4：分批推帧（4 批 × 批间 300ms ≈ 900ms）+ 播前静默窗 + presynth 豁免
//     flowToken（消费端 barge-in 反向 gateway 掐断在途推帧的链路被斩断）
//   - 批次 A-6：批间 300ms → 50ms（3 个批间隔 ≈150ms）——浏览器播完一批等 300ms 会
//     underrun 卡顿；50ms 保持推帧持续防回声 barge-in，又消除卡顿
// 断言：
//   1) bridge 链路实测：108 帧推帧总耗时 ≈150ms（>100ms 分批生效，<500ms 非老 300ms 节奏/慢推）
//   2) presynthFrames 纯函数：108 帧分 4 批、3 个批间隔 ≈50ms、帧切分内容正确
//   3) 源码回归：presynthFrames 分批签名（默认批间 50）+ presynth 命中分支豁免 flowToken（方案 C）
//      + miss 分支保留 flowToken + 播前静默窗（方案 B）+ A-2 慢推回归
// 用法: node scripts/presynth-pacing-test.cjs
// 退出码: 0 = 全部通过, 1 = 存在失败

const assert = require('node:assert/strict');

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** 纳秒级时间戳（Node 事件循环一轮可能 <1ms，Date.now 精度不够）。 */
const hrtimeNs = () => Number(process.hrtime.bigint());

/** 轮询等待谓词为真。 */
async function waitFor(predicate, timeoutMs = 8000, intervalMs = 20) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (predicate()) return true;
    if (Date.now() >= deadline) return false;
    await sleep(intervalMs);
  }
}

const MOCK_FRAME = Buffer.alloc(960, 7);

/** 真实「稍等，我查一下」实测：108 帧 / 103680 字节 / 2.160s（24kHz/16bit/mono）。 */
const PRESYNTH_FRAMES = 108;
const PRESYNTH_BYTES = PRESYNTH_FRAMES * 960;

/** mock TTS：synthesize 返回单块 mp3 数据（等价 mp3mock）。 */
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

/** mock decoder：把 stream 解码成 pcm16 帧。 */
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

function createMockLlmFactory(reply = '真实回复') {
  return () => ({
    streamChat: async function* () {
      yield { content: reply };
    },
  });
}

/** 构造 108 帧（103680 字节 pcm16，等价真实「稍等，我查一下」）的预合成缓存条目。 */
function buildPresynthCacheEntry() {
  const chunks = [];
  for (let i = 0; i < PRESYNTH_FRAMES; i += 1) chunks.push(Buffer.alloc(960, i % 256));
  return Buffer.concat(chunks);
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

  const { createBridgeRuntime, presynthFrames } = await import('../src/bridge/bridge-runtime.js');

  // ---------- 测试 1：bridge 链路实测——预合成命中 → 108 帧分批推帧 ----------
  await test('预合成推帧：分批推帧（108 帧总耗时 >100ms 且 <500ms，批间 50ms）', async () => {
    const events = [];
    const frameTimes = [];
    const bigEntry = buildPresynthCacheEntry();

    // 大块 tts + decoder：warmup 预合成产出 103680 字节 pcm16（等价 108 帧预合成 buffer）
    const ttsFactory = (() => {
      const calls = [];
      const shared = {
        synthesize(text) {
          calls.push(text);
          return (async function* () {
            yield { type: 'audio', data: Buffer.from(`big:${text}`) };
          })();
        },
        getCalls: () => [...calls],
      };
      return () => shared;
    })();
    const decoderFactory = () => ({
      reset: async () => {},
      decode: async function* (stream) {
        for await (const chunk of stream) {
          if (chunk?.type === 'audio') {
            yield { pcm16: bigEntry, sampleRate: 24000 }; // 一次产出全部 pcm
          }
        }
      },
      free: () => {},
    });

    const bridge = createBridgeRuntime({
      providerConfig: { baseUrl: 'http://mock', apiKey: 'k', model: 'm' },
      placeholderDelayMs: 0,
      llmFactory: createMockLlmFactory(),
      ttsFactory,
      decoderFactory,
      onTranscript: () => {},
      onAudio: () => {
        frameTimes.push(hrtimeNs());
        events.push({ type: 'audio' });
      },
      onMark: () => {},
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    await bridge.connect();
    await sleep(150); // warmup
    // 直接触发等待语：占位文本 → 预合成命中（warmup 已合成「稍等，我查一下」）
    bridge.sendUserMessage('Briefly tell the person that you are checking with OpenClaw.');
    await waitFor(() => events.filter((e) => e.type === 'audio').length >= PRESYNTH_FRAMES, 8000);
    const total = frameTimes.length;
    assert.ok(total >= PRESYNTH_FRAMES, `应推满 ${PRESYNTH_FRAMES} 帧，实际 ${total}`);
    const first = frameTimes[0];
    const last = frameTimes[total - 1];
    const elapsed = last - first;
    // 分批推帧节奏：≈150ms（4 批 × 3 个批间隔 50ms）——不是同步爆发（≈0ms），
    // 也不是老 300ms 节奏（≈900ms）或 setTimeout(20) 匀速慢推（≈2140ms）
    assert.ok(elapsed > 100e6, `108 帧总耗时应 >100ms（分批生效），实际 ${(elapsed / 1e6).toFixed(0)}ms`);
    assert.ok(elapsed < 500e6, `108 帧总耗时应 <500ms（非老 300ms 节奏/匀速慢推），实际 ${(elapsed / 1e6).toFixed(0)}ms`);
    // 首末帧时间戳应不同：108 帧不是在同一瞬间全部推完
    assert.ok(last > first, '首末帧时间戳应不同（非同一 tick 爆发）');
    console.log(
      `  实测: ${total} 帧, 总耗时 ${(elapsed / 1e6).toFixed(0)}ms` +
        ` (预期分批: >100ms 且 <500ms; 同步≈0ms / 老300ms节奏≈900ms / setTimeout(20)≈2140ms)`
    );
    bridge.close();
  });

  // ---------- 测试 2：presynthFrames 纯函数——分批节奏 + 帧切分正确 ----------
  await test('presynthFrames 纯函数：108 帧分 4 批、批间隔 ≈50ms、帧切分正确', async () => {
    const pcm = buildPresynthCacheEntry();
    const timestamps = [];
    const frames = [];
    for await (const frame of presynthFrames(pcm, 960)) {
      timestamps.push(hrtimeNs());
      frames.push(frame);
    }
    assert.equal(frames.length, PRESYNTH_FRAMES, '帧数应为 108');
    const elapsed = timestamps[timestamps.length - 1] - timestamps[0];
    // 分批推帧：108 帧 → 4 批（每批 27 帧）× 3 个批间隔 50ms ≈ 150ms
    assert.ok(elapsed > 100e6, `总耗时应 >100ms（分批生效），实际 ${(elapsed / 1e6).toFixed(0)}ms`);
    assert.ok(elapsed < 500e6, `总耗时应 <500ms（非老 300ms 节奏/匀速慢推），实际 ${(elapsed / 1e6).toFixed(0)}ms`);
    // 帧切分正确：每帧 960 字节，逐帧拼接 == 原 buffer
    assert.equal(frames[0].length, 960, '每帧应为 960 字节');
    const rebuilt = Buffer.concat(frames.map((f) => Buffer.from(f)));
    assert.ok(rebuilt.equals(pcm), '逐帧拼接应与原 buffer 一致');
    // 批间间隔检测：帧间时间戳应有 3 处 >30ms 的大跳（批边界 = 50ms timer；
    // 批内微任务连推 gap <5ms，与批间隔显著可分）
    const gaps = [];
    for (let i = 1; i < timestamps.length; i += 1) {
      gaps.push(timestamps[i] - timestamps[i - 1]);
    }
    const bigGaps = gaps.filter((g) => g > 30e6);
    assert.equal(bigGaps.length, 3, `应有 3 个批间隔，实际 ${bigGaps.length}`);
    const avgBatchGap = bigGaps.reduce((a, b) => a + b, 0) / bigGaps.length;
    assert.ok(avgBatchGap > 30e6 && avgBatchGap < 200e6, `批间隔应 ≈50ms，实际 ${(avgBatchGap / 1e6).toFixed(0)}ms`);
    console.log(
      `  实测: ${frames.length} 帧, 总耗时 ${(elapsed / 1e6).toFixed(0)}ms, 批间隔 ${(avgBatchGap / 1e6).toFixed(0)}ms, ` +
        `每帧 ${frames[0].length}B (预期: 4 批 × 50ms ≈ 150ms)`
    );
  });

  // ---------- 测试 3：源码回归——分批签名 + 方案 C 豁免 + 方案 B 静默窗 + A-2 回归 ----------
  await test('源码回归：presynth 分批签名 + 命中分支豁免 flowToken + miss 分支保留 + 播前静默窗 + 无 setTimeout(20) 慢推', async () => {
    const fs = await import('node:fs');
    const src = fs.readFileSync('src/bridge/bridge-runtime.js', 'utf8');

    // A-4：presynthFrames 分批签名（export 便于单测，默认批间隔 50ms——批次 A-6 收缩）
    assert.ok(
      /export async function\* presynthFrames\(pcm, frameSize, batchIntervalMs = 50\)/.test(src),
      '应存在 export async function* presynthFrames(pcm, frameSize, batchIntervalMs = 50)'
    );
    // A-4：playDirectTts 用 for await 调用 presynthFrames
    assert.ok(src.includes('for await (const frame of presynthFrames('), 'playDirectTts 应用 for await 调用 presynthFrames');

    // 方案 C：presynth 命中分支只检查 closed，不含 flowToken 打断检查
    const pStart = src.indexOf('const presynthPcm = getPresynthCache().get(text);');
    assert.ok(pStart >= 0, '应找到 presynth 推帧块');
    const pEnd = src.indexOf('} else {', pStart);
    assert.ok(pEnd > pStart, '应找到 presynth 分支结束（miss 分支开始）');
    const pBlock = src.slice(pStart, pEnd);
    assert.ok(pBlock.includes('if (closed) return'), 'presynth 命中分支应只检查 closed（方案 C 豁免 flowToken）');
    assert.ok(!pBlock.includes('token !== flowToken'), 'presynth 命中分支不应含 flowToken 打断检查（方案 C）');

    // 方案 C 边界：miss 分支保留 flowToken 检查（实时合成路径保守）
    assert.ok(src.includes('if (synthToken !== flowToken) return;'), 'miss 分支应保留 flowToken 检查');
    // 推帧后收尾：presynth 豁免后 flowToken 可能已变，用 closed 收尾
    assert.ok(src.includes('if (closed) return; // 批次 A-4 方案 C'), '推帧后收尾应改用 closed 检查（方案 C）');
    // playDirectTts 开头调用时检查保留
    const callStart = src.indexOf('async function playDirectTts(text, token)');
    const callBlock = src.slice(callStart, callStart + 400);
    assert.ok(callBlock.includes('if (token !== flowToken) return;'), 'playDirectTts 开头应保留调用时 token 检查');

    // 方案 B：playWaitMessage 播前静默窗（PRESYNTH_PLAY_PRE_DELAY_MS）
    assert.ok(src.includes('const PRESYNTH_PLAY_PRE_DELAY_MS = 300;'), '应存在播前静默窗常量 PRESYNTH_PLAY_PRE_DELAY_MS = 300');
    assert.ok(
      /setTimeout\(\(\) => \{[\s\S]*?playDirectTts\(placeholderReplacementText, token\)[\s\S]*?\}, PRESYNTH_PLAY_PRE_DELAY_MS\);/.test(src),
      'playWaitMessage 应含播前静默窗（300ms 后再 playDirectTts）'
    );

    // A-2 慢推回归：推帧循环里不应再有 setTimeout(resolve, 20)（20ms 慢推 = 完全不播的回归）
    assert.ok(!src.includes('setTimeout(resolve, 20)'), 'A-2 回归：推帧不应再含 setTimeout(20) 慢推');
    assert.ok(!src.includes('setImmediate('), '不应再含 setImmediate 让步调用');
    // 打断检查必须在 safeCall 之前（presynth 分支：closed 检查在推帧前）
    const checkPos = pBlock.indexOf('if (closed) return');
    const callPos = pBlock.indexOf('safeCall(onAudio');
    assert.ok(checkPos >= 0 && callPos > checkPos, 'closed 检查应在推帧之前');
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
