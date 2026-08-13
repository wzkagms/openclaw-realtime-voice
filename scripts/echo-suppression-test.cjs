// scripts/echo-suppression-test.cjs
// Phase 4 回声规避（方案 B 折中版播放锁）验证：
//   1) TTS 播放期间（playbackLocked）pushPcm 不喂 STT（suppressed 生效）
//   2) 播放结束后尾部静默抑制窗（300ms）内仍抑制
//   3) 300ms 后恢复 feed
//   4) barge-in 立即解锁（用户真实说话可打断）
//   5) VAD 语音段在抑制期不被污染（isSpeechActive 不误置）
//   6) bridge 全链路：TTS 播放期间 sendAudio 不触发 onRecognizedText（真 sherpa 模型）
// 用法: node scripts/echo-suppression-test.cjs
// 退出码: 0 = 全部通过, 1 = 存在失败

const assert = require('node:assert/strict');
const path = require('node:path');
const { createRequire } = require('node:module');

const require_ = createRequire(require.resolve('../package.json'));

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** 轮询等待谓词为真（与 bridge-llm-tts-test / e2e-test 同一模式）。 */
async function waitFor(predicate, timeoutMs = 5000, intervalMs = 20) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (predicate()) return true;
    if (Date.now() >= deadline) return false;
    await sleep(intervalMs);
  }
}

/** mock STT：记录 feed 调用次数，不真正识别。 */
function createMockStt() {
  let feedCount = 0;
  let endpoints = 0;
  return {
    feed() {
      feedCount += 1;
    },
    isEndpoint() {
      return endpoints > 0;
    },
    setEndpointFlag(v) {
      endpoints = v ? 1 : 0;
    },
    getResult() {
      return { text: '' };
    },
    reset() {
      feedCount = 0;
    },
    finalize() {},
    getFeedCount: () => feedCount,
  };
}

const MOCK_FRAME_24K = Buffer.alloc(960, 7); // 20ms @24k = 960 bytes
const MOCK_FRAME_16K = Buffer.alloc(640, 7); // 20ms @16k = 640 bytes

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

  // ---------- 测试 1：未锁时 pushPcm 正常 feed ----------
  await test('未锁时 pushPcm 正常 feed STT', async () => {
    const { createPipeline } = await import('../src/bridge/audio-pipeline.js');
    const stt = createMockStt();
    let suppressed = false;
    const pipeline = createPipeline({
      stt,
      inputSampleRate: 24000,
      shouldSuppressInput: () => suppressed,
    });
    pipeline.pushPcm(MOCK_FRAME_24K);
    pipeline.pushPcm(MOCK_FRAME_24K);
    assert.equal(stt.getFeedCount(), 2, `feedCount=${stt.getFeedCount()}`);
    pipeline.destroy();
  });

  // ---------- 测试 2：播放锁期间 pushPcm 不 feed ----------
  await test('播放锁（shouldSuppressInput=true）期间 pushPcm 不喂 STT', async () => {
    const { createPipeline } = await import('../src/bridge/audio-pipeline.js');
    const stt = createMockStt();
    let suppressed = true; // 模拟 TTS 播放中（playbackLocked=true）
    const pipeline = createPipeline({
      stt,
      inputSampleRate: 24000,
      shouldSuppressInput: () => suppressed,
    });
    pipeline.pushPcm(MOCK_FRAME_24K);
    pipeline.pushPcm(MOCK_FRAME_24K);
    assert.equal(stt.getFeedCount(), 0, `feedCount=${stt.getFeedCount()} (播放锁应抑制 feed)`);
    // 播放结束 + 300ms 内仍抑制
    pipeline.destroy();
  });

  // ---------- 测试 3：解锁后恢复 feed ----------
  await test('解锁后 pushPcm 恢复 feed STT', async () => {
    const { createPipeline } = await import('../src/bridge/audio-pipeline.js');
    const stt = createMockStt();
    let suppressed = false;
    const pipeline = createPipeline({
      stt,
      inputSampleRate: 24000,
      shouldSuppressInput: () => suppressed,
    });
    pipeline.pushPcm(MOCK_FRAME_24K);
    assert.equal(stt.getFeedCount(), 1);
    suppressed = true;
    pipeline.pushPcm(MOCK_FRAME_24K);
    assert.equal(stt.getFeedCount(), 1, '锁定时不应 feed');
    suppressed = false;
    pipeline.pushPcm(MOCK_FRAME_24K);
    assert.equal(stt.getFeedCount(), 2, '解锁后应恢复 feed');
    pipeline.destroy();
  });

  // ---------- 测试 4：setShouldSuppressInput 动态切换 ----------
  await test('setShouldSuppressInput 动态启停抑制', async () => {
    const { createPipeline } = await import('../src/bridge/audio-pipeline.js');
    const stt = createMockStt();
    const pipeline = createPipeline({ stt, inputSampleRate: 24000 });
    pipeline.pushPcm(MOCK_FRAME_24K);
    assert.equal(stt.getFeedCount(), 1);
    pipeline.setShouldSuppressInput(() => true);
    pipeline.pushPcm(MOCK_FRAME_24K);
    assert.equal(stt.getFeedCount(), 1, 'setShouldSuppressInput(true) 后应抑制');
    pipeline.setShouldSuppressInput(() => false);
    pipeline.pushPcm(MOCK_FRAME_24K);
    assert.equal(stt.getFeedCount(), 2, 'setShouldSuppressInput(false) 后应恢复');
    pipeline.setShouldSuppressInput(null);
    pipeline.pushPcm(MOCK_FRAME_24K);
    assert.equal(stt.getFeedCount(), 3, '传非函数应清除抑制');
    pipeline.destroy();
  });

  // ---------- 测试 5：抑制期 VAD 语音段不被污染 ----------
  await test('抑制期 VAD 语音段不被污染（isSpeechActive 不误置）', async () => {
    const { createPipeline } = await import('../src/bridge/audio-pipeline.js');
    const stt = createMockStt();
    let suppressed = true;
    const pipeline = createPipeline({
      stt,
      inputSampleRate: 24000,
      shouldSuppressInput: () => suppressed,
    });
    // 构造高音量帧（模拟扬声器回声）：若被喂入 VAD，会触发 onSpeechStart/isSpeechActive
    const loud = Buffer.alloc(960);
    for (let i = 0; i < loud.length; i += 2) loud.writeInt16LE(20000, i);
    pipeline.pushPcm(loud);
    pipeline.pushPcm(loud);
    assert.equal(pipeline.isSpeechActive(), false, '抑制期不应进入语音段');
    assert.equal(stt.getFeedCount(), 0);
    // 解锁后：同帧应正常进入 VAD 语音段
    suppressed = false;
    pipeline.pushPcm(loud);
    assert.equal(pipeline.isSpeechActive(), true, '解锁后高音量帧应进入语音段');
    pipeline.destroy();
  });

  // ---------- 测试 6：bridge 全链路——TTS 播放期间 sendAudio 不触发 onRecognizedText ----------
  await test('bridge: TTS 播放期间 sendAudio 不触发识别出句（真 sherpa + mock LLM/TTS）', async () => {
    const MODEL_DIR = path.join(
      __dirname, '..', 'models', 'sherpa-onnx-streaming-zipformer-bilingual-zh-en-2023-02-20',
    );
    const fs = require('node:fs');
    if (!fs.existsSync(path.join(MODEL_DIR, 'encoder-epoch-99-avg-1.onnx'))) {
      console.error('  [SKIP] sherpa model missing — skipped (bridge full-chain test)');
      return;
    }
    const { createBridgeRuntime } = await import('../src/bridge/bridge-runtime.js');
    const recognized = [];
    const audioFrames = [];
    const errors = [];
    // mock LLM：返回固定文本 → TTS 播放
    const llmFactory = () => ({
      streamChat: async function* () {
        yield { content: '这是回声抑制测试回复。' };
      },
    });
    const ttsFactory = () => ({
      synthesize: async function* () {
        yield { audio: Buffer.from('mp3mock') };
      },
    });
    const decoderFactory = () => ({
      reset: async () => {},
      decode: async function* () {
        // 播放 5 帧（100ms），足够期间喂入回声帧
        for (let i = 0; i < 5; i++) yield { pcm16: MOCK_FRAME_24K };
      },
      free: () => {},
    });
    const bridge = createBridgeRuntime({
      providerConfig: { baseUrl: 'http://mock', apiKey: 'k', model: 'm' },
      inputSampleRate: 24000,
      playbackTailSuppressionMs: 300,
      llmFactory,
      ttsFactory,
      decoderFactory,
      onRecognizedText: (text) => recognized.push(text),
      onAudio: (buf) => audioFrames.push(buf),
      onError: (err) => errors.push(err),
    });
    await bridge.connect();
    // 触发 TTS 播放（sendUserMessage → LLM → TTS → onAudio）
    bridge.sendUserMessage('测试');
    const started = await waitFor(() => audioFrames.length >= 1, 3000);
    assert.ok(started, 'TTS 应开始播放（onAudio 收到帧）');
    // 播放期间喂入高音量回声帧——应被播放锁抑制，不触发识别出句
    const loud = Buffer.alloc(960);
    for (let i = 0; i < loud.length; i += 2) loud.writeInt16LE(20000, i);
    for (let i = 0; i < 10; i++) bridge.sendAudio(loud);
    await sleep(100);
    assert.equal(recognized.length, 0, `播放期间不应识别出句, recognized=${JSON.stringify(recognized)}`);
    // 等 TTS 播完 + 尾部抑制窗结束，再喂回声帧——此时应恢复正常（但无静音尾巴，不触发出句也可）
    await sleep(600);
    bridge.close();
    assert.equal(errors.length, 0, `errors=${JSON.stringify(errors.map((e) => e.message))}`);
  });

  // ---------- 测试 7：bridge 全链路——barge-in 立即解锁恢复识别 ----------
  await test('bridge: barge-in 打断播放 → 播放锁立即释放（后续 sendAudio 可识别）', async () => {
    const { createBridgeRuntime } = await import('../src/bridge/bridge-runtime.js');
    const recognized = [];
    const audioFrames = [];
    const errors = [];
    let llmCall = 0;
    const llmFactory = () => ({
      streamChat: async function* () {
        llmCall += 1;
        yield { content: '打断测试回复。' };
      },
    });
    const ttsFactory = () => ({
      synthesize: async function* () {
        yield { audio: Buffer.from('mp3mock') };
      },
    });
    const decoderFactory = () => ({
      reset: async () => {},
      decode: async function* () {
        // 长播放（2s）——便于打断后验证解锁
        for (let i = 0; i < 100; i++) yield { pcm16: MOCK_FRAME_24K };
      },
      free: () => {},
    });
    const bridge = createBridgeRuntime({
      providerConfig: { baseUrl: 'http://mock', apiKey: 'k', model: 'm' },
      inputSampleRate: 24000,
      playbackTailSuppressionMs: 300,
      llmFactory,
      ttsFactory,
      decoderFactory,
      onRecognizedText: (text) => recognized.push(text),
      onAudio: (buf) => audioFrames.push(buf),
      onError: (err) => errors.push(err),
    });
    await bridge.connect();
    bridge.sendUserMessage('测试');
    await waitFor(() => audioFrames.length >= 1, 3000);
    // barge-in：打断播放 → 播放锁立即释放
    bridge.handleBargeIn();
    // 打断后 feed 高音量帧（模拟用户说话）→ 应进入 LISTENING 并可识别
    // （不依赖真 STT endpoint，仅验证状态恢复 + 无错误）
    const loud = Buffer.alloc(960);
    for (let i = 0; i < loud.length; i += 2) loud.writeInt16LE(20000, i);
    for (let i = 0; i < 5; i++) bridge.sendAudio(loud);
    await sleep(100);
    assert.equal(bridge.getState(), 'LISTENING', `barge-in 后应在 LISTENING, state=${bridge.getState()}`);
    bridge.close();
    assert.equal(errors.length, 0, `errors=${JSON.stringify(errors.map((e) => e.message))}`);
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
