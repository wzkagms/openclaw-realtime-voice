// scripts/utterance-filter-test.cjs
// 批次 B 最短 utterance 过滤验证：
//   1) isIgnorableUtterance 纯函数：单字非白名单 → 忽略；白名单单字/正常句/空 → 保留
//   2) 白名单全量覆盖（嗯/好/在/是/对/行/唉）
//   3) 源码级链路校验：handleSpeechEnd 中 isIgnorableUtterance 过滤必须早于
//      handleRecognizedText（保证单字噪音不触发识别上报/consult）
// 说明：handleSpeechEnd 是 createBridgeRuntime 闭包内函数，且 createBridgeRuntime 无
// sttFactory 注入点（真 sherpa 模型本仓库不存在 → 全链路不可行），故测纯函数 + 链路
// 位置校验（与批次 B 验收要求一致）。
// 用法: node scripts/utterance-filter-test.cjs
// 退出码: 0 = 全部通过, 1 = 存在失败

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

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

  const { isIgnorableUtterance } = await import('../src/bridge/bridge-runtime.js');

  // ---------- 纯函数：单字非白名单 → 忽略（true） ----------
  await test('单字噪音「的」→ 忽略 (true)', () => {
    assert.equal(isIgnorableUtterance('的'), true);
  });
  await test('单字噪音「啊」→ 忽略 (true)', () => {
    assert.equal(isIgnorableUtterance('啊'), true);
  });
  await test('单字噪音「a」（单 ASCII 字母）→ 忽略 (true)', () => {
    assert.equal(isIgnorableUtterance('a'), true);
  });

  // ---------- 纯函数：白名单单字 → 保留（false） ----------
  await test('白名单「好」→ 保留 (false)', () => {
    assert.equal(isIgnorableUtterance('好'), false);
  });
  await test('白名单「在」→ 保留 (false)', () => {
    assert.equal(isIgnorableUtterance('在'), false);
  });
  await test('白名单全量（嗯/是/对/行/唉）→ 保留 (false)', () => {
    for (const ch of ['嗯', '是', '对', '行', '唉']) {
      assert.equal(isIgnorableUtterance(ch), false, `白名单字「${ch}」不应被忽略`);
    }
  });

  // ---------- 纯函数：正常多字句 → 保留（false） ----------
  await test('正常句「今天天气怎么样」→ 保留 (false)', () => {
    assert.equal(isIgnorableUtterance('今天天气怎么样'), false);
  });
  await test('多字应答「好的」→ 保留 (false)', () => {
    assert.equal(isIgnorableUtterance('好的'), false);
  });

  // ---------- 纯函数：空/非字符串 → 保留（false，走原有逻辑） ----------
  await test('空文本 "" → 保留 (false，走原有无文本分支)', () => {
    assert.equal(isIgnorableUtterance(''), false);
  });
  await test('undefined → 保留 (false，非字符串不判忽略)', () => {
    assert.equal(isIgnorableUtterance(undefined), false);
  });
  await test('非字符串 123 → 保留 (false)', () => {
    assert.equal(isIgnorableUtterance(123), false);
  });
  await test('含空格的串 "   " → 保留 (false；真实路径已 trim，纯函数对多字符不判忽略)', () => {
    assert.equal(isIgnorableUtterance('   '), false);
  });

  // ---------- 链路校验：过滤必须早于 handleRecognizedText ----------
  await test('链路: handleSpeechEnd 中 isIgnorableUtterance 过滤早于 handleRecognizedText', () => {
    const src = fs.readFileSync(
      path.join(__dirname, '..', 'src', 'bridge', 'bridge-runtime.js'),
      'utf8',
    );
    // 白名单常量存在（源码校验，防误删）
    assert.ok(
      src.includes("const SINGLE_CHAR_WHITELIST = Object.freeze(['嗯', '好', '在', '是', '对', '行', '唉']);"),
      'SINGLE_CHAR_WHITELIST 常量缺失',
    );
    const start = src.indexOf('function handleSpeechEnd()');
    assert.ok(start !== -1, 'handleSpeechEnd 函数缺失');
    const end = src.indexOf('function ensureInitialized', start);
    const body = src.slice(start, end);
    const filterPos = body.indexOf('isIgnorableUtterance(text)');
    const recognizedPos = body.indexOf('handleRecognizedText(text)');
    assert.ok(filterPos !== -1, 'handleSpeechEnd 中缺少 isIgnorableUtterance(text) 过滤');
    assert.ok(recognizedPos !== -1, 'handleSpeechEnd 中缺少 handleRecognizedText 调用');
    assert.ok(filterPos < recognizedPos, '过滤必须位于 handleRecognizedText 之前（单字噪音不应上报/consult）');
    // 丢弃分支应走 RESET 回 IDLE（与无文本分支一致）
    const filterBlock = body.slice(filterPos, filterPos + 160);
    assert.ok(filterBlock.includes('EVENTS.RESET'), '单字噪音丢弃分支应 RESET 回 IDLE');
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
