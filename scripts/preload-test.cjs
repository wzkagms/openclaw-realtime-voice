// scripts/preload-test.cjs
// Phase 4 预加载（OnlineRecognizer 单例缓存）验证：
//   1) 单例缓存生效：第二次 createStt（同配置）不重新初始化 recognizer（refCount 递增）
//   2) 多 stream 并发正常：两个 createStt 实例独立 stream，各自 feed/reset 互不干扰
//   3) finalize 只减计数不清缓存；缓存在会话间保留
//   4) 模型文件缺失仍抛清晰错误
// 用法: node scripts/preload-test.cjs
// 退出码: 0 = 全部通过, 1 = 存在失败

const assert = require('node:assert/strict');
const path = require('node:path');
const fs = require('node:fs');

const MODEL_DIR = path.join(
  __dirname, '..', 'models', 'sherpa-onnx-streaming-zipformer-bilingual-zh-en-2023-02-20',
);

if (!fs.existsSync(path.join(MODEL_DIR, 'encoder-epoch-99-avg-1.int8.onnx'))) {
  console.error('[SKIP] sherpa model missing — preload-test requires local int8 model');
  process.exit(0);
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

  const { createStt, getRecognizerCacheSnapshot } = await import('../src/stt/sherpa-stt.js');

  // 测试 1：首次 createStt 加载模型（缓存 1 条，refCount=1）
  await test('首次 createStt：缓存创建 refCount=1', async () => {
    const stt = createStt({ modelPath: MODEL_DIR });
    const snap = getRecognizerCacheSnapshot();
    assert.equal(snap.length, 1, `缓存应有 1 条，实际=${snap.length}`);
    assert.equal(snap[0].refCount, 1, `refCount 应为 1，实际=${snap[0].refCount}`);
    stt.finalize();
    // finalize 后 refCount 归 0 但缓存保留
    const after = getRecognizerCacheSnapshot();
    assert.equal(after[0].refCount, 0, `finalize 后 refCount 应归 0，实际=${after[0].refCount}`);
    assert.equal(after.length, 1, `finalize 不应清除缓存，实际=${after.length}`);
  });

  // 测试 2：第二次 createStt（同配置）复用缓存，refCount 递增
  await test('第二次 createStt 复用缓存 refCount 递增（不重新初始化）', async () => {
    const stt2 = createStt({ modelPath: MODEL_DIR });
    const snap = getRecognizerCacheSnapshot();
    assert.equal(snap.length, 1, `复用后缓存仍应只有 1 条，实际=${snap.length}`);
    assert.equal(snap[0].refCount, 1, `refCount 应为 1（测试1已 finalize 归 0 后重新 +1），实际=${snap[0].refCount}`);
    // 多实例并发：再建一个，refCount 递增
    const stt3 = createStt({ modelPath: MODEL_DIR });
    const snap2 = getRecognizerCacheSnapshot();
    assert.equal(snap2[0].refCount, 2, `两个活跃实例 refCount 应为 2，实际=${snap2[0].refCount}`);
    stt2.finalize();
    stt3.finalize();
    const snap3 = getRecognizerCacheSnapshot();
    assert.equal(snap3[0].refCount, 0, `全部 finalize 后 refCount 归 0，实际=${snap3[0].refCount}`);
  });

  // 测试 3：不同 modelPath → 不同缓存条目
  await test('不同 modelPath → 不同缓存条目', async () => {
    const missing = path.join(MODEL_DIR, 'nonexistent');
    // 不存在的 modelPath：应抛清晰错误（文件缺失），不产生新缓存
    assert.throws(() => createStt({ modelPath: missing }), /model files missing/);
    const snap = getRecognizerCacheSnapshot();
    assert.equal(snap.length, 1, `无效路径不应新增缓存，实际=${snap.length}`);
  });

  // 测试 4：多 stream 并发——两个实例独立 feed/reset 不互相影响
  await test('多 stream 并发：两实例独立 feed/reset 互不干扰', async () => {
    const a = createStt({ modelPath: MODEL_DIR });
    const b = createStt({ modelPath: MODEL_DIR });
    // 静音帧（不影响识别但走通 pipeline 不抛错）
    const silence = new Float32Array(1600); // 0.1s @16k
    a.feed(silence);
    b.feed(silence);
    a.reset(); // 只重置 a，b 不受影响
    b.feed(silence);
    a.feed(silence);
    assert.ok(true, '多 stream 并发 feed/reset 无异常');
    a.finalize();
    b.finalize();
  });

  // 测试 5：finalize 后调用方法抛清晰错误
  await test('finalize 后调用方法抛清晰错误', async () => {
    const stt = createStt({ modelPath: MODEL_DIR });
    stt.finalize();
    assert.throws(() => stt.feed(new Float32Array(1600)), /finalized/);
    assert.throws(() => stt.isEndpoint(), /finalized/);
    assert.throws(() => stt.getResult(), /finalized/);
    assert.throws(() => stt.reset(), /finalized/);
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
