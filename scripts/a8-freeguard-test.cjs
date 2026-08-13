// scripts/a8-freeguard-test.cjs
// 批次 A-8 修复验证（free 校验 + isCancelled 放弃在途预合成）：
//   根因：bridge close() free 掉 mpg123 decoder 后，connect warmup 触发的在途预合成
//   （presynth-cache.synthesizeToBuffer，无取消机制）继续对已释放的 WASM decoder 调
//   decode() → mpg123 返回 -1 不推进 → MPEGDecoder 内层 read 循环无限 console.error
//   （~180/s）→ 同步死循环阻塞 gateway 事件循环（129098 条 MPG123_ERR）。
//
//   治标（decode-pipeline）：collectAudioChunks（await 点）之后、decoder.decode 之前
//   校验 decoder !== decoderInstance → 抛错一次性结束（不进入死循环）。
//   治本（presynth-cache + bridge-runtime）：isCancelled 三处检查（合成前 / 每 chunk 后 /
//   合成完成后），close 后放弃在途合成，根本不让 decode() 触碰已 free 的 decoder。
//
// 用法: node scripts/a8-freeguard-test.cjs
// 退出码: 0 = 全部通过, 1 = 存在失败

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** 轮询等待谓词为真。 */
async function waitFor(predicate, timeoutMs = 10000, intervalMs = 20) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (predicate()) return true;
    if (Date.now() >= deadline) return false;
    await sleep(intervalMs);
  }
}

/** 外部可控的一次性信号门闩。 */
function makeGate() {
  let resolve;
  const promise = new Promise((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

/** 可挂起的 async 流：yield 首个 chunk 后 await gate，release 后 yield 第二个 chunk 结束。 */
function createGatedStream() {
  let started = false;
  const gate = makeGate();
  const stream = (async function* () {
    yield { type: 'audio', data: Buffer.from('mp3-part-1') };
    started = true;
    await gate.promise;
    yield { type: 'audio', data: Buffer.from('mp3-part-2') };
  })();
  return { stream, release: () => gate.resolve(), isStarted: () => started };
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

  // ---------- 测试 1（治标）：free 后 decode → 抛错一次结束，不死循环 ----------
  await test('治标：decoder free 后 decode 抛「decoder was freed」（一次结束不死循环）', async () => {
    const { createDecodePipeline } = await import('../src/tts/decode-pipeline.js');
    const pipeline = createDecodePipeline();
    const gated = createGatedStream();
    // decode 是 async generator：next() 开始执行 → getDecoder() → collectAudioChunks
    // → 挂起在 gated stream 的 await 点（模拟 edge-tts 在途合成 7s 期间用户取消）
    const gen = pipeline.decode(gated.stream);
    const nextPromise = gen.next();
    const started = await waitFor(() => gated.isStarted());
    assert.ok(started, 'decode 应已挂起在 collectAudioChunks（stream 已开始）');
    // 用户取消 → bridge close → decoder.free()（decoderInstance 置 null）
    pipeline.free();
    // 在途合成完成 → collectAudioChunks 返回 → 治标校验发现 decoder 已 free → 抛错
    gated.release();
    await assert.rejects(nextPromise, /decoder was freed during synthesis/);
    // 证明不是死循环：generator 已终止，后续 next() 立即 done（而非继续读悬空指针）
    const tail = await gen.next();
    assert.equal(tail.done, true, 'decode generator 应已终止（无死循环）');
  });

  // ---------- 测试 2（治本·场景 A）：isCancelled 初始 true → 不合成 ----------
  await test('治本 A：isCancelled()=true 时不合成（synthesize 不被调用）', async () => {
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
      isCancelled: () => true, // close 后状态
    });
    cache.get('稍等，我查一下'); // 触发（应被取消）
    await cache.flush();
    assert.equal(synthCount, 0, `isCancelled=true 时不应调用 synthesize（实际 ${synthCount}）`);
    assert.equal(cache.get('稍等，我查一下'), null, '缓存不应写入 pcm16');
  });

  // ---------- 测试 3（治本·场景 B）：合成中取消（每 chunk 后检查）→ 放弃不写缓存 ----------
  await test('治本 B：合成中 isCancelled 变 true → 放弃（缓存不写入）', async () => {
    const { createPresynthCache } = await import('../src/tts/presynth-cache.js');
    let synthCount = 0;
    const midGate = makeGate();
    const tts = {
      synthesize(text) {
        synthCount += 1;
        return (async function* () {
          yield { type: 'audio', data: Buffer.from('mp3:part1') };
          await midGate.promise; // 模拟 edge-tts 流式合成中途挂起
          yield { type: 'audio', data: Buffer.from('mp3:part2') };
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
    const cancelled = { value: false };
    const cache = createPresynthCache({
      tts,
      decoder,
      gate,
      providerSignature: { voice: 'v', baseUrl: 'http://a', model: 'm' },
      isCancelled: () => cancelled.value,
    });
    cache.get('稍等，我查一下'); // 触发预合成
    // 等第一个 chunk 已被 decode 消费（tts 挂起在 midGate）→ 模拟 close：置取消标志
    // waitFor 轮询周期内 microtask 链已走完（frame1 push + isCancelled 检查 false），
    // 此刻设置 cancelled 恰好落在「每 chunk 后检查点」之间
    await waitFor(() => synthCount === 1);
    cancelled.value = true; // close() 置 closed = true
    midGate.resolve(); // 在途合成继续 → 下一 chunk → 检查 isCancelled true → 放弃
    await cache.flush();
    assert.equal(synthCount, 1, '合成已启动（1 次调用）');
    assert.equal(cache.get('稍等，我查一下'), null, '取消后缓存不应写入 pcm16');
  });

  // ---------- 测试 4（治本·场景 C）：合成完成但已取消 → 丢弃缓存不写入 ----------
  await test('治本 C：合成完成但 isCancelled 变 true → 丢弃缓存（不写入）', async () => {
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
    const endGate = makeGate();
    const decoder = {
      decode: async function* (stream) {
        for await (const chunk of stream) {
          if (chunk?.type === 'audio') {
            yield { pcm16: Buffer.from(chunk.data), sampleRate: 24000 };
            await endGate.promise; // decode 完成最后一个 frame 后挂起，模拟 close 插入窗口
          }
        }
      },
    };
    const gate = (fn) => fn();
    const cancelled = { value: false };
    const cache = createPresynthCache({
      tts,
      decoder,
      gate,
      providerSignature: { voice: 'v', baseUrl: 'http://a', model: 'm' },
      isCancelled: () => cancelled.value,
    });
    cache.get('稍等，我查一下');
    // 等 tts 合成完成（单 chunk 已 yield）→ decode 挂起在 endGate → 此刻置取消
    await waitFor(() => synthCount === 1);
    cancelled.value = true; // 合成完成后、写缓存前取消
    endGate.resolve(); // decode 结束 → Buffer.concat → 检查 isCancelled true → 丢弃
    await cache.flush();
    assert.equal(cache.get('稍等，我查一下'), null, '取消后缓存不应写入 pcm16');
  });

  // ---------- 测试 5（源码回归）：三处设计点存在 ----------
  await test('源码回归：decode-pipeline free 校验 / presynth-cache 三处 isCancelled / bridge 传参', () => {
    const read = (rel) => fs.readFileSync(path.join(__dirname, '..', rel), 'utf8');
    const decodeSrc = read('src/tts/decode-pipeline.js');
    assert.match(decodeSrc, /decoder !== decoderInstance/, 'decode-pipeline 应含 decoder !== decoderInstance 校验');
    assert.match(decodeSrc, /decoder was freed during synthesis/, 'decode-pipeline 应抛「decoder was freed」');
    const presynthSrc = read('src/tts/presynth-cache.js');
    const checks = presynthSrc.match(/isCancelled\?\.\(\)/g) || [];
    assert.ok(checks.length >= 3, `presynth-cache 应含 3 处 isCancelled?.() 检查（实际 ${checks.length}）`);
    const bridgeSrc = read('src/bridge/bridge-runtime.js');
    assert.match(bridgeSrc, /isCancelled: \(\) => closed/, 'bridge getPresynthCache 应传 isCancelled: () => closed');
  });

  // ---------- 回归：presynth-cache 不传 isCancelled（可选参数）仍正常 ----------
  await test('回归：presynth-cache 不传 isCancelled（可选）→ 正常预合成', async () => {
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
      // 故意不传 isCancelled：验证可选参数向后兼容
    });
    cache.get('稍等，我查一下');
    await cache.flush();
    assert.equal(synthCount, 1, '不传 isCancelled 时应正常合成');
    assert.ok(Buffer.isBuffer(cache.get('稍等，我查一下')), '缓存应正常写入');
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
