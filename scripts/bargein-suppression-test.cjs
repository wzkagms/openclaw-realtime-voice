// scripts/bargein-suppression-test.cjs
// 批次 C barge-in 回声抑制修复验证 + 批次 A-5 force consult 清场区分验证：
//   根因（C）：用户打断时 handleBargeIn 立即解锁播放锁，但扬声器物理声音仍有输出延迟
//   （锁解了声音还在 → 回声尾巴混入麦克风 → 句首识别被污染）。
//   修复（C）：播放中打断（playbackLocked=true）保持锁 + 复用 schedulePlaybackUnlock()
//   追加 300ms 抑制窗再解锁；非播放中打断（无回声）保持原语义直接解锁。
//   根因（A-5）：gateway 强制 consult 机制在每次出句上报后 200ms 必发
//   handleBargeIn({force:true})，它递增 flowToken 掐断等待语（300ms 静默窗后播）→ 完全不播。
//   修复（A-5）：handleBargeIn(options) 区分 force（强制 consult 清场，只清浏览器缓冲）
//   vs 真实 barge-in（完整打断语义原样保留）。
//
// 验证策略（与批次 B 惯例一致）：
//   1) 源码级链路校验：handleBargeIn 分支结构 + 原有逻辑保留 + schedulePlaybackUnlock
//      复用 + 「只改一处」护栏（置位/复位/调用点计数不漂移）+ force 分支边界校验
//      （只含 safeCall(onClearAudio)+return，不含 flowToken/定时器清理/状态转换）
//   2) 实例冒烟：handleBargeIn 暴露可调用、不抛错、onClearAudio 触发、IDLE+BARGE_IN
//      非法转换被 canTransition 保护（原语义）；force 调用不抛错且 state 不变
//   说明：playbackLocked/flowToken/placeholderTimer 是 createBridgeRuntime 闭包变量，无
//   观测接口；createStt 无 factory 注入点且真模型本仓库不存在 → 全链路/行为级时序不可行
//   （同批次 B 验收约束），force 分支的 token/定时器边界走源码级校验。
// 用法: node scripts/bargein-suppression-test.cjs
// 退出码: 0 = 全部通过, 1 = 存在失败

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const RUNTIME_PATH = path.join(__dirname, '..', 'src', 'bridge', 'bridge-runtime.js');

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
  const src = fs.readFileSync(RUNTIME_PATH, 'utf8');

  // ---------- 提取 handleBargeIn 函数体 ----------
  const fnStart = src.indexOf('function handleBargeIn(');
  assert.ok(fnStart !== -1, 'handleBargeIn 函数缺失');
  const bodyStart = src.indexOf('{', fnStart);
  const fnEnd = src.indexOf('function close()', fnStart);
  assert.ok(fnEnd !== -1, 'handleBargeIn 函数边界缺失（close 未找到）');
  const body = src.slice(bodyStart, fnEnd);

  // ---------- A. 源码级：批次 A-5 force consult 清场分支 ----------
  await test('源码: force 分支存在且位于函数开头（options?.force === true）', () => {
    assert.ok(body.includes('if (options?.force === true) {'), '缺少 force 分支');
    const forcePos = body.indexOf('if (options?.force === true) {');
    const flowPos = body.indexOf('flowToken += 1;');
    assert.ok(forcePos !== -1 && forcePos < flowPos, 'force 分支必须先于真实路径（早返回）');
  });

  await test('源码: force 分支只清缓冲 + return，不含 flowToken/classifyToken/定时器/状态转换/解锁', () => {
    const forceStart = body.indexOf('if (options?.force === true) {');
    assert.ok(forceStart !== -1, 'force 分支缺失');
    // 分支结束 = 真实路径首行（flowToken += 1）之前；若 force 分支缺失 flowToken，
    // 这里取到分支自身闭合；用「分支起点 → flowToken 起点」区间做边界校验。
    const forceEnd = body.indexOf('flowToken += 1;', forceStart);
    assert.ok(forceEnd !== -1, '真实路径 flowToken += 1 缺失');
    const forceBody = body.slice(forceStart, forceEnd);
    // 必须包含：清浏览器缓冲 + return
    assert.ok(forceBody.includes("safeCall(onClearAudio, 'barge-in');"), 'force 分支必须调 onClearAudio');
    assert.ok(forceBody.includes('return;'), 'force 分支必须早返回');
    // 必须不含：token 递增 / 定时器清理 / 状态转换 / 解锁
    for (const forbidden of [
      'flowToken += 1;',
      'classifyToken += 1;',
      'clearConsultFallbackTimer();',
      'clearPlaceholderTimer();',
      'canTransition(state, EVENTS.BARGE_IN)',
      'playbackLocked = false;',
      'schedulePlaybackUnlock();',
    ]) {
      assert.ok(!forceBody.includes(forbidden), `force 分支不应包含: ${forbidden}`);
    }
  });

  await test('源码: force 分支 safeCall(onClearAudio) 与 return 顺序正确（先清缓冲再返回）', () => {
    const forceStart = body.indexOf('if (options?.force === true) {');
    const clearPos = body.indexOf("safeCall(onClearAudio, 'barge-in');", forceStart);
    const returnPos = body.indexOf('return;', clearPos);
    assert.ok(clearPos !== -1 && returnPos !== -1, 'force 分支缺少 onClearAudio 或 return');
    assert.ok(clearPos < returnPos, 'force 分支必须先 onClearAudio 再 return');
  });

  await test('源码: 真实路径保留 flowToken++ 且 force 分支不含 flowToken（共 1 处）', () => {
    const forceStart = body.indexOf('if (options?.force === true) {');
    const forceEnd = body.indexOf('flowToken += 1;', forceStart);
    const flowCount = (body.match(/flowToken \+= 1;/g) || []).length;
    assert.equal(flowCount, 1, `真实路径应恰好 1 处 flowToken += 1（force 分支不得含），实际 ${flowCount}`);
    assert.ok(forceEnd !== -1, '真实路径 flowToken += 1 缺失');
  });

  // ---------- A. 源码级：批次 C 修复分支结构 ----------
  await test('源码: handleBargeIn 存在且含修复分支 if (playbackLocked)', () => {
    assert.ok(body.includes('if (playbackLocked) {'), '缺少 playbackLocked 分支');
    assert.ok(body.includes('if (playbackLocked) {\n      schedulePlaybackUnlock();\n    } else {'), '分支结构异常');
  });

  await test('源码: 播放中打断 → 复用 schedulePlaybackUnlock()（不立即解锁）', () => {
    const branchPos = body.indexOf('if (playbackLocked) {');
    const callPos = body.indexOf('schedulePlaybackUnlock();');
    assert.ok(branchPos !== -1, 'if (playbackLocked) 分支缺失');
    assert.ok(callPos !== -1, '分支内 schedulePlaybackUnlock() 缺失');
    assert.ok(callPos > branchPos, 'schedulePlaybackUnlock() 必须在 playbackLocked 分支内');
    const elsePos = body.indexOf('} else {', branchPos);
    assert.ok(elsePos > callPos, 'else 分支必须位于 schedulePlaybackUnlock() 之后');
  });

  await test('源码: 非播放中打断 → else 直接解锁（原语义保留）', () => {
    const elsePos = body.indexOf('} else {');
    const unlockPos = body.indexOf('playbackLocked = false;');
    assert.ok(elsePos !== -1, 'else 分支缺失');
    assert.ok(unlockPos !== -1, 'else 分支直接解锁缺失');
    assert.ok(unlockPos > elsePos, 'playbackLocked = false 必须在 else 分支内');
  });

  await test('源码: clearPlaybackUnlockTimer 在分支之前（打断先清旧抑制窗）', () => {
    const clearPos = body.indexOf('clearPlaybackUnlockTimer();');
    const branchPos = body.indexOf('if (playbackLocked) {');
    assert.ok(clearPos !== -1, 'clearPlaybackUnlockTimer() 缺失');
    assert.ok(clearPos < branchPos, 'clearPlaybackUnlockTimer 必须先于分支执行');
  });

  // ---------- A. 源码级：原有逻辑全部保留 ----------
  await test('源码: 原有逻辑保留（flowToken/consult/placeholder/classify/onClearAudio/BARGE_IN）', () => {
    for (const fragment of [
      'flowToken += 1;',
      'clearConsultFallbackTimer();',
      'clearPlaceholderTimer();',
      'classifyToken += 1;',
      "safeCall(onClearAudio, 'barge-in');",
      'canTransition(state, EVENTS.BARGE_IN)',
    ]) {
      assert.ok(body.includes(fragment), `handleBargeIn 丢失原逻辑片段: ${fragment}`);
    }
  });

  // ---------- A. 源码级：schedulePlaybackUnlock 定义未改动 ----------
  await test('源码: schedulePlaybackUnlock 定义未改动（仍用 playbackTailSuppressionMs，未新增常量）', () => {
    const defStart = src.indexOf('function schedulePlaybackUnlock()');
    assert.ok(defStart !== -1, 'schedulePlaybackUnlock 定义缺失');
    const defEnd = src.indexOf('\n  }', defStart) + 4;
    const defBody = src.slice(defStart, defEnd);
    assert.ok(defBody.includes('playbackTailSuppressionMs'), 'schedulePlaybackUnlock 未使用 playbackTailSuppressionMs');
    assert.ok(defBody.includes('setTimeout('), 'schedulePlaybackUnlock 未使用 setTimeout');
    assert.ok(!defBody.includes('playbackTailSuppressionMs = '), 'schedulePlaybackUnlock 内不应重新赋值常量');
  });

  // ---------- A. 源码级：「只改一处」护栏 ----------
  await test('源码: schedulePlaybackUnlock() 调用点仍为 3 处（原 788/864 + 新 921），无新增定时器', () => {
    const callCount = (src.match(/schedulePlaybackUnlock\(\);/g) || []).length;
    assert.equal(callCount, 3, `期望 3 处调用，实际 ${callCount}`);
  });

  await test('源码: playbackLocked 置位点仍为 2 处（759/819，runLlmTtsFlow/playDirectTts 未动）', () => {
    const lockCount = (src.match(/playbackLocked = true;/g) || []).length;
    assert.equal(lockCount, 2, `期望 2 处置位，实际 ${lockCount}`);
  });

  await test('源码: handleBargeIn 内 playbackLocked 复位仅 1 处（else 分支）', () => {
    const unlockCount = (body.match(/playbackLocked = false;/g) || []).length;
    assert.equal(unlockCount, 1, `期望 1 处复位（else 分支），实际 ${unlockCount}`);
  });

  // ---------- B. 实例冒烟 ----------
  await test('实例: handleBargeIn 暴露在返回对象', () => {
    const rt = createBridgeRuntime({ providerConfig: {} });
    assert.equal(typeof rt.handleBargeIn, 'function');
    rt.close();
  });

  await test('实例: IDLE 态调用 handleBargeIn 不抛错 + onClearAudio 触发 + state 保持（IDLE+BARGE_IN 非法，canTransition 保护原语义）', () => {
    const clearCalls = [];
    const rt = createBridgeRuntime({ providerConfig: {}, onClearAudio: (reason) => clearCalls.push(reason) });
    assert.equal(rt.getState(), 'IDLE');
    rt.handleBargeIn();
    assert.equal(rt.getState(), 'IDLE', 'IDLE 态打断不应转换（原语义：canTransition 保护）');
    assert.deepEqual(clearCalls, ['barge-in'], 'onClearAudio 应以 barge-in 触发');
    rt.close();
  });

  await test('实例: 重复打断幂等（不抛错、onClearAudio 计数递增）', () => {
    const clearCalls = [];
    const rt = createBridgeRuntime({ providerConfig: {}, onClearAudio: (reason) => clearCalls.push(reason) });
    rt.handleBargeIn();
    rt.handleBargeIn();
    rt.handleBargeIn();
    assert.equal(clearCalls.length, 3, '每次打断都应触发 onClearAudio');
    assert.deepEqual(clearCalls, ['barge-in', 'barge-in', 'barge-in']);
    rt.close();
  });

  // ---------- B. 实例冒烟：批次 A-5 force consult 清场 ----------
  await test('实例: force 调用不抛错 + onClearAudio 触发 + state 不变（force 不做状态转换）', () => {
    const clearCalls = [];
    const rt = createBridgeRuntime({ providerConfig: {}, onClearAudio: (reason) => clearCalls.push(reason) });
    assert.equal(rt.getState(), 'IDLE');
    rt.handleBargeIn({ force: true, audioPlaybackActive: true });
    assert.equal(rt.getState(), 'IDLE', 'force 清场不应做状态转换');
    assert.deepEqual(clearCalls, ['barge-in'], 'force 清场应触发 onClearAudio（只清缓冲）');
    rt.close();
  });

  await test('实例: force 调用不改变真实打断语义（force 后真实打断完整生效）', () => {
    const clearCalls = [];
    const rt = createBridgeRuntime({ providerConfig: {}, onClearAudio: (reason) => clearCalls.push(reason) });
    rt.handleBargeIn({ force: true, audioPlaybackActive: true });
    rt.handleBargeIn(); // 真实打断：完整链路
    assert.equal(clearCalls.length, 2, 'force + 真实打断各触发一次 onClearAudio');
    assert.deepEqual(clearCalls, ['barge-in', 'barge-in']);
    rt.close();
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
