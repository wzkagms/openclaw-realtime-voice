// scripts/state-machine-test.cjs
// 状态机全路径测试（Phase 1 bridge 核心，subtask 04）
// 用法: node scripts/state-machine-test.cjs
// 退出码: 0 = 全部通过, 1 = 存在失败
//
// 说明: scripts/ 是 CJS（.cjs），src/ 是 ESM（type:module），
// CJS 无法 require ESM，这里用动态 import 加载状态机模块，
// 并在 async main() 中执行（.cjs 不支持顶层 await）。

const assert = require('node:assert/strict');

async function main() {
  const { STATES, EVENTS, transition, canTransition } = await import('../src/bridge/state-machine.js');

  let passed = 0;
  let failed = 0;
  const failures = [];

  function test(name, fn) {
    try {
      fn();
      passed += 1;
      console.log(`PASS: ${name}`);
    } catch (err) {
      failed += 1;
      failures.push({ name, err });
      console.log(`FAIL: ${name}`);
    }
  }

  // ---- 合法迁移路径 ----
  test('IDLE + INPUT_START -> LISTENING', () => {
    assert.equal(transition(STATES.IDLE, EVENTS.INPUT_START), STATES.LISTENING);
  });

  test('LISTENING + SPEECH_END -> RECOGNIZING', () => {
    assert.equal(transition(STATES.LISTENING, EVENTS.SPEECH_END), STATES.RECOGNIZING);
  });

  test('RECOGNIZING + RECOGNIZED -> THINKING', () => {
    assert.equal(transition(STATES.RECOGNIZING, EVENTS.RECOGNIZED), STATES.THINKING);
  });

  // ---- 打断边界（BARGE_IN 回到 LISTENING）----
  test('SPEAKING + BARGE_IN -> LISTENING (barge-in)', () => {
    assert.equal(transition(STATES.SPEAKING, EVENTS.BARGE_IN), STATES.LISTENING);
  });

  test('RECOGNIZING + BARGE_IN -> LISTENING (barge-in)', () => {
    assert.equal(transition(STATES.RECOGNIZING, EVENTS.BARGE_IN), STATES.LISTENING);
  });

  test('THINKING + BARGE_IN -> LISTENING (barge-in)', () => {
    assert.equal(transition(STATES.THINKING, EVENTS.BARGE_IN), STATES.LISTENING);
  });

  test('LISTENING + BARGE_IN -> LISTENING (idempotent)', () => {
    assert.equal(transition(STATES.LISTENING, EVENTS.BARGE_IN), STATES.LISTENING);
  });

  // ---- RESET / CLOSE 从任意合法状态回到 IDLE ----
  for (const state of Object.values(STATES)) {
    test(`${state} + RESET -> IDLE`, () => {
      assert.equal(transition(state, EVENTS.RESET), STATES.IDLE);
    });
    test(`${state} + CLOSE -> IDLE`, () => {
      assert.equal(transition(state, EVENTS.CLOSE), STATES.IDLE);
    });
  }

  // ---- 非法迁移抛错 ----
  test('IDLE + RECOGNIZED throws Invalid transition', () => {
    assert.throws(
      () => transition(STATES.IDLE, EVENTS.RECOGNIZED),
      /Invalid transition: IDLE \+ RECOGNIZED/
    );
  });

  test('IDLE + SPEECH_END throws Invalid transition', () => {
    assert.throws(
      () => transition(STATES.IDLE, EVENTS.SPEECH_END),
      /Invalid transition: IDLE \+ SPEECH_END/
    );
  });

  test('LISTENING + RECOGNIZED throws Invalid transition', () => {
    assert.throws(
      () => transition(STATES.LISTENING, EVENTS.RECOGNIZED),
      /Invalid transition: LISTENING \+ RECOGNIZED/
    );
  });

  test('transition(undefined, RESET) throws Unknown state', () => {
    assert.throws(() => transition(undefined, EVENTS.RESET), /Unknown state: undefined/);
  });

  test('transition(IDLE, NOT_A_REAL_EVENT) throws Unknown event', () => {
    assert.throws(() => transition(STATES.IDLE, 'NOT_A_REAL_EVENT'), /Unknown event: NOT_A_REAL_EVENT/);
  });

  // ---- canTransition（guard 不抛错）----
  test('canTransition(IDLE, INPUT_START) === true', () => {
    assert.equal(canTransition(STATES.IDLE, EVENTS.INPUT_START), true);
  });

  test('canTransition(IDLE, RECOGNIZED) === false (no throw)', () => {
    assert.equal(canTransition(STATES.IDLE, EVENTS.RECOGNIZED), false);
  });

  test("canTransition('BOGUS', RESET) === false (no throw)", () => {
    assert.equal(canTransition('BOGUS', EVENTS.RESET), false);
  });

  // ---- TTS / 文本输入路径（subtask 08 补充）----
  test('THINKING + TTS_START -> SPEAKING', () => {
    assert.equal(transition(STATES.THINKING, EVENTS.TTS_START), STATES.SPEAKING);
  });

  test('SPEAKING + TTS_END -> IDLE', () => {
    assert.equal(transition(STATES.SPEAKING, EVENTS.TTS_END), STATES.IDLE);
  });

  test('IDLE + TEXT_INPUT -> THINKING', () => {
    assert.equal(transition(STATES.IDLE, EVENTS.TEXT_INPUT), STATES.THINKING);
  });

  test('canTransition(THINKING, TTS_START) === true', () => {
    assert.equal(canTransition(STATES.THINKING, EVENTS.TTS_START), true);
  });

  test('canTransition(SPEAKING, TTS_END) === true', () => {
    assert.equal(canTransition(STATES.SPEAKING, EVENTS.TTS_END), true);
  });

  // 非法迁移：guard 不抛错，直接返回 false
  test('canTransition(IDLE, TTS_START) === false (no throw)', () => {
    assert.equal(canTransition(STATES.IDLE, EVENTS.TTS_START), false);
  });

  test('canTransition(THINKING, TEXT_INPUT) === false (no throw)', () => {
    assert.equal(canTransition(STATES.THINKING, EVENTS.TEXT_INPUT), false);
  });

  test('canTransition(LISTENING, TTS_START) === false (no throw)', () => {
    assert.equal(canTransition(STATES.LISTENING, EVENTS.TTS_START), false);
  });

  // ---- 汇总 ----
  console.log(`\n${passed} passed, ${failed} failed`);
  if (failed > 0) {
    console.log('\nFailed cases:');
    for (const { name, err } of failures) {
      console.log(`  [${name}]`);
      console.log(`    message: ${err.message}`);
      if (err.expected !== undefined) {
        console.log(`    expected: ${JSON.stringify(err.expected)}`);
        console.log(`    actual:   ${JSON.stringify(err.actual)}`);
      }
    }
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
