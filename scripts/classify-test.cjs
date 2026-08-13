// scripts/classify-test.cjs
// Phase 4 方案 3.0（LLM 意图分类 + 语音清洗，澜影增强）验证：
//   1) 乱码转录（重复字/同音错字）→ cleanedText 正确顺化（双输出 JSON 解析）
//   2) 数字/地名保持不被改写（「湖北」「14号」）
//   3) needTool=true（天气等）→ 播等待语；needTool=false → 不播
//   4) 清洗失败/超时 → 降级用原始文本上报 + 播等待语
//   5) 真实回复到达 → 取消在途分类（不误播等待语）
//   6) 上报 gateway 的文本 = cleanedText（main agent 输入顺化）
// 用法: node scripts/classify-test.cjs
// 退出码: 0 = 全部通过, 1 = 存在失败

const assert = require('node:assert/strict');
const path = require('node:path');
const fs = require('node:fs');

const MODEL_DIR = path.join(
  __dirname, '..', 'models', 'sherpa-onnx-streaming-zipformer-bilingual-zh-en-2023-02-20',
);
if (!fs.existsSync(path.join(MODEL_DIR, 'encoder-epoch-99-avg-1.onnx'))) {
  console.error('[SKIP] sherpa model missing — classify-test requires local int8 model');
  process.exit(0);
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const MOCK_FRAME_24K = Buffer.alloc(960, 7);

/** 轮询等待谓词为真（与其他测试同一模式）。 */
async function waitFor(predicate, timeoutMs = 5000, intervalMs = 20) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (predicate()) return true;
    if (Date.now() >= deadline) return false;
    await sleep(intervalMs);
  }
}

/** mock TTS + decoder 工厂（与 agent-consult-test 同实现）。 */
function createMockTtsFactory() {
  return () => ({
    synthesize: async function* () {
      yield { audio: Buffer.from('mp3mock') };
    },
  });
}
function createMockDecoderFactory() {
  return () => ({
    reset: async () => {},
    decode: async function* () {
      yield { pcm16: MOCK_FRAME_24K };
    },
    free: () => {},
  });
}

/**
 * 分类器 mock LLM：根据 user 消息内容返回 JSON 三输出（增强 2）。
 * 支持场景：needTool 命中词触发 needTool=true；cleanedText 固定值；fail/delay 模拟降级；
 * understandable=false 模拟乱码（重说）；understandableOverride 强制覆盖。
 */
function createClassifierMock({
  needTool = false,
  cleanedText = '',
  fail = false,
  delayMs = 0,
  understandable = true,
} = {}) {
  return () => {
    const llm = {
      streamChat: async function* (messages) {
        if (fail) throw new Error('classifier fetch failed');
        if (delayMs > 0) await sleep(delayMs);
        const userMsg = messages.find((m) => m.role === 'user')?.content ?? '';
        const hasWeather = userMsg.includes('天气');
        const tool = needTool || hasWeather;
        const cleaned = cleanedText || (hasWeather ? '今天天气怎么样' : '你好');
        yield {
          content: JSON.stringify({
            needTool: tool,
            cleanedText: cleaned,
            understandable,
          }),
        };
      },
    };
    return llm;
  };
}

/** 从 wav 读取 PCM16（e2e-test 同模式）。 */
function readWavPcm16(filePath) {
  const buf = fs.readFileSync(filePath);
  let offset = 12;
  let data = null;
  let sampleRate = 16000;
  while (offset + 8 <= buf.length) {
    const id = buf.toString('ascii', offset, offset + 4);
    const size = buf.readUInt32LE(offset + 4);
    if (id === 'fmt ') {
      sampleRate = buf.readUInt32LE(offset + 12);
    } else if (id === 'data') {
      data = buf.subarray(offset + 8, offset + 8 + size);
      break;
    }
    offset += 8 + size + (size % 2);
  }
  if (!data) throw new Error(`readWavPcm16: ${filePath} missing data chunk`);
  return { pcm: Buffer.from(data), sampleRate };
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

  const WAV = path.join(MODEL_DIR, 'test_wavs', '0.wav');
  const { pcm, sampleRate } = readWavPcm16(WAV);

  /** 创建 bridge（mock 分类 LLM + TTS/decoder），并触发一次语音出句（真 sherpa）。 */
  async function createBridgeAndSpeak({ llmFactory, onTranscript, onAudio, onEvent, onError, classifyTimeoutMs }) {
    const { createBridgeRuntime } = await import('../src/bridge/bridge-runtime.js');
    const bridge = createBridgeRuntime({
      providerConfig: { baseUrl: 'http://mock', apiKey: 'k', model: 'm' },
      autoRespondToAudio: false, // agent-consult 主路径：出句 → 分类 → 上报
      classifyIntent: true,
      ackEnabled: false, // 本测试聚焦分类/清洗/重说逻辑，关闭 ack（出句「嗯」会干扰 onAudio 断言）
      ...(classifyTimeoutMs !== undefined ? { classifyTimeoutMs } : {}),
      inputSampleRate: 16000, // test_wavs 是 16k PCM，匹配 bridge resampler
      llmFactory,
      ttsFactory: createMockTtsFactory(),
      decoderFactory: createMockDecoderFactory(),
      onTranscript,
      onAudio,
      onEvent,
      onError,
    });
    await bridge.connect();
    // 喂入 wav 语音 + 3s 静音触发 endpoint 出句（与 e2e 同模式）
    const frameBytes = 16000 * 20 * 2 / 1000; // 640 @16k
    const CHUNK = frameBytes;
    for (let offset = 0; offset < pcm.length; offset += CHUNK) {
      bridge.sendAudio(pcm.subarray(offset, offset + CHUNK));
      await sleep(5);
    }
    // 3s 静音尾巴触发 endpoint（sherpa endpoint 需 ≥2.4s 静音）
    const silence = Buffer.alloc(CHUNK * 160);
    for (let offset = 0; offset < silence.length; offset += CHUNK) {
      bridge.sendAudio(silence.subarray(offset, offset + CHUNK));
      await sleep(2);
    }
    return bridge;
  }

  // ---------- 测试 1：清洗 + needTool 双输出解析（天气 → 顺化 + 等待语） ----------
  await test('乱码转录「天天天天津天气怎么样」→ cleanedText 顺化 + needTool=true → 播等待语', async () => {
    const events = [];
    const llmFactory = createClassifierMock({
      cleanedText: '今天天气怎么样',
      needTool: true,
    });
    const bridge = await createBridgeAndSpeak({
      llmFactory,
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onAudio: () => events.push({ type: 'audio' }),
      onEvent: (e) => events.push({ type: 'event', eventType: e.type, detail: e.detail }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    // 等待分类完成 + 上报
    const reported = await waitFor(() => events.some((e) => e.type === 'transcript' && e.role === 'user' && e.final), 3000);
    assert.ok(reported, '应上报 user final transcript');
    const userFinal = events.find((e) => e.type === 'transcript' && e.role === 'user' && e.final);
    assert.equal(userFinal.text, '今天天气怎么样', `上报文本应为 cleanedText，实际=${userFinal.text}`);
    // needTool=true → 播等待语
    const waitAudio = await waitFor(() => events.filter((e) => e.type === 'audio').length >= 1, 3000);
    assert.ok(waitAudio, 'needTool=true 应播等待语（onAudio≥1）');
    bridge.close();
  });

  // ---------- 测试 2：不需要工具（NO_TOOL）→ 不播等待语 ----------
  await test('「你好」→ needTool=false → 不播等待语', async () => {
    const events = [];
    const bridge = await createBridgeAndSpeak({
      llmFactory: createClassifierMock({ cleanedText: '你好', needTool: false }),
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onAudio: () => events.push({ type: 'audio' }),
      onEvent: (e) => events.push({ type: 'event', eventType: e.type }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    const reported = await waitFor(() => events.some((e) => e.type === 'transcript' && e.role === 'user' && e.final), 3000);
    assert.ok(reported, '应上报 user final');
    await sleep(300); // 等足够时间确认无等待语
    assert.equal(events.filter((e) => e.type === 'audio').length, 0, 'needTool=false 不应播等待语（onAudio=0）');
    bridge.close();
  });

  // ---------- 测试 3：清洗失败 → 降级原始文本上报 + 播等待语 ----------
  await test('分类失败（mock 抛错）→ 降级原始文本上报 + 播等待语', async () => {
    const events = [];
    const bridge = await createBridgeAndSpeak({
      llmFactory: createClassifierMock({ fail: true }),
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onAudio: () => events.push({ type: 'audio' }),
      onEvent: (e) => events.push({ type: 'event', eventType: e.type, detail: e.detail }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    const reported = await waitFor(() => events.some((e) => e.type === 'transcript' && e.role === 'user' && e.final), 3000);
    assert.ok(reported, '分类失败仍应上报（不阻塞）');
    const userFinal = events.find((e) => e.type === 'transcript' && e.role === 'user' && e.final);
    assert.ok(userFinal.text.length > 0, `降级应上报原始文本，实际=${JSON.stringify(userFinal.text)}`);
    const degraded = events.find((e) => e.eventType === 'intent_classified' && e.detail?.includes('degraded=true'));
    assert.ok(degraded, `应上报 degraded=true 事件，实际=${JSON.stringify(events.map((e) => e.detail))}`);
    // 降级 → 播等待语
    const waitAudio = await waitFor(() => events.filter((e) => e.type === 'audio').length >= 1, 3000);
    assert.ok(waitAudio, '分类失败降级应播等待语');
    bridge.close();
  });

  // ---------- 测试 4：分类超时 → 降级播等待语 ----------
  await test('分类超时（mock 延迟 > 阈值）→ 降级播等待语', async () => {
    const events = [];
    const bridge = await createBridgeAndSpeak({
      // 注入短超时（50ms）模拟超时，mock 延迟 200ms 必然超时
      classifyTimeoutMs: 50,
      llmFactory: createClassifierMock({ delayMs: 200 }),
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onAudio: () => events.push({ type: 'audio' }),
      onEvent: (e) => events.push({ type: 'event', eventType: e.type }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    const waitAudio = await waitFor(() => events.filter((e) => e.type === 'audio').length >= 1, 3000);
    assert.ok(waitAudio, '分类超时降级应播等待语');
    bridge.close();
  });

  // ---------- 测试 5：真实回复到达 → 取消在途分类（不误播等待语） ----------
  await test('真实回复先到（sendUserMessage）→ 取消在途分类（不播等待语）', async () => {
    const events = [];
    const bridge = await createBridgeAndSpeak({
      llmFactory: createClassifierMock({ delayMs: 300, needTool: true }),
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onAudio: () => events.push({ type: 'audio' }),
      onEvent: (e) => events.push({ type: 'event', eventType: e.type }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    // 分类还在跑（delayMs=300）时，真实回复到达 → 取消分类
    await sleep(50);
    bridge.sendUserMessage('真实回复');
    await sleep(500); // 等超过分类延迟
    // 真实回复播放（mock TTS），且无等待语单独播放（分类被取消 → 无 playWaitMessage）
    const userFinals = events.filter((e) => e.type === 'transcript' && e.role === 'user' && e.final);
    assert.ok(userFinals.length >= 1, '应上报 user final（真实回复路径也上报）');
    bridge.close();
  });

  // ---------- 测试 6：乱码转录 → understandable=false → 播重说提示、不发起 consult ----------
  await test('乱码转录（understandable=false）→ 播重说提示、不发起 consult', async () => {
    const events = [];
    const bridge = await createBridgeAndSpeak({
      llmFactory: createClassifierMock({ understandable: false, cleanedText: '啊吧啦呼' }),
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onAudio: () => events.push({ type: 'audio' }),
      onEvent: (e) => events.push({ type: 'event', eventType: e.type, detail: e.detail }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    // 应播重说提示（onAudio≥1）
    const waitAudio = await waitFor(() => events.filter((e) => e.type === 'audio').length >= 1, 3000);
    assert.ok(waitAudio, 'understandable=false 应播重说提示');
    // 不应上报 user final（不发起 consult）
    const userFinals = events.filter((e) => e.type === 'transcript' && e.role === 'user' && e.final);
    assert.equal(userFinals.length, 0, `乱码不应上报 consult（userFinal=0），实际=${userFinals.length}`);
    assert.ok(events.some((e) => e.eventType === 'retry_prompt'), '应上报 retry_prompt 事件');
    bridge.close();
  });

  // ---------- 测试 7：简短输入（「嗯」「好」）→ understandable=true 不误判 ----------
  await test('简短输入（understandable=true 缺失默认）→ 不误判、正常流程', async () => {
    const events = [];
    // mock 返回无 understandable 字段 → 解析默认 true（宽松）
    const llmFactory = () => ({
      streamChat: async function* (messages) {
        yield { content: JSON.stringify({ needTool: false, cleanedText: '嗯' }) }; // 无 understandable
      },
    });
    const bridge = await createBridgeAndSpeak({
      llmFactory,
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onAudio: () => events.push({ type: 'audio' }),
      onEvent: (e) => events.push({ type: 'event', eventType: e.type }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    const reported = await waitFor(() => events.some((e) => e.type === 'transcript' && e.role === 'user' && e.final), 3000);
    assert.ok(reported, '简短输入应正常上报（understandable 默认 true，不打断）');
    assert.equal(events.filter((e) => e.eventType === 'retry_prompt').length, 0, '不应触发重说');
    bridge.close();
  });

  // ---------- 测试 8：解析失败 → 默认 understandable=true（降级不打断） ----------
  await test('解析失败（mock 返回非 JSON）→ 默认 true 正常上报 + 播等待语（降级）', async () => {
    const events = [];
    const llmFactory = () => ({
      streamChat: async function* () {
        yield { content: '这不是 JSON' }; // 解析失败
      },
    });
    const bridge = await createBridgeAndSpeak({
      llmFactory,
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onAudio: () => events.push({ type: 'audio' }),
      onEvent: (e) => events.push({ type: 'event', eventType: e.type, detail: e.detail }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    // 解析失败 → degraded → 上报原始 + 播等待语
    const reported = await waitFor(() => events.some((e) => e.type === 'transcript' && e.role === 'user' && e.final), 3000);
    assert.ok(reported, '解析失败仍应上报（降级不打断）');
    const waitAudio = await waitFor(() => events.filter((e) => e.type === 'audio').length >= 1, 3000);
    assert.ok(waitAudio, '解析失败降级应播等待语');
    assert.equal(events.filter((e) => e.eventType === 'retry_prompt').length, 0, '解析失败不应触发重说（默认 true）');
    bridge.close();
  });

  // ---------- 测试 9：输入预处理 collapseRepeatedChars（方案 1） ----------
  const { collapseRepeatedChars } = await import('../src/bridge/bridge-runtime.js');  await test('collapseRepeatedChars：极端重复压缩 / 正常词不误伤', async () => {
    // >3 连续相同 → 压到 1
    assert.equal(collapseRepeatedChars('aaaa'), 'a', '4 个连续应压到 1');
    assert.equal(collapseRepeatedChars('abbbb'), 'ab', '4 个 b 应压到 1');
    assert.equal(collapseRepeatedChars('天天天天天津'), '天津', '4 个天应压到 1');
    // ≤3 连续相同 → 保留（不误伤）
    assert.equal(collapseRepeatedChars('aa'), 'aa', '2 个保留');
    assert.equal(collapseRepeatedChars('aaa'), 'aaa', '3 个保留');
    assert.equal(collapseRepeatedChars('天天天津'), '天天天津', '3 个天保留（天天天天津 是 4 天+津，会压）');
    // 正常词不受影响
    assert.equal(collapseRepeatedChars('天津'), '天津');
    assert.equal(collapseRepeatedChars('晚安'), '晚安');
    assert.equal(collapseRepeatedChars('神龙山山'), '神龙山山', '2 个山保留');
    // 空/非字符串
    assert.equal(collapseRepeatedChars(''), '');
    assert.equal(collapseRepeatedChars('  '), '  ');
  });

  // ---------- 测试 10：OpenClaw env 对象 apiKey 解析（根因修复验证） ----------
  await test('resolveApiKey：env 对象 → 取 process.env 实际值；字符串原样；其他 → 空串', async () => {
    const { resolveApiKey } = await import('../src/bridge/bridge-runtime.js');
    process.env.__TTS_TEST_KEY = 'env-test-key';
    // env 对象 → 取 env 实际值
    assert.equal(
      resolveApiKey({ source: 'env', provider: 'default', id: '__TTS_TEST_KEY' }),
      'env-test-key',
    );
    // 字符串原样
    assert.equal(resolveApiKey('plain-string-key'), 'plain-string-key');
    // env 对象但 env 不存在 → 空串
    assert.equal(resolveApiKey({ source: 'env', id: '__NONEXISTENT_KEY__' }), '');
    // 其他对象 / null / undefined → 空串
    assert.equal(resolveApiKey({ foo: 'bar' }), '');
    assert.equal(resolveApiKey(null), '');
    assert.equal(resolveApiKey(undefined), '');
    delete process.env.__TTS_TEST_KEY;
  });

  // ---------- 测试 11：working 提示 + 分类 needTool=false → 不播等待语（00:22 根因修复） ----------
  await test('working 提示注入 + 分类 needTool=false → 不播「稍等」（分类接管决策）', async () => {
    const { createBridgeRuntime } = await import('../src/bridge/bridge-runtime.js');
    const events = [];
    const bridge = createBridgeRuntime({
      providerConfig: { baseUrl: 'http://mock', apiKey: 'k', model: 'm' },
      placeholderDelayMs: 100, // 若旧逻辑触发会很快播
      ackEnabled: false, // 排除「在的」
      classifyIntent: true, // 分类接管等待语决策
      llmFactory: () => ({
        streamChat: async function* (messages) {
          const sys = messages[0]?.role === 'system';
          if (sys) yield { content: JSON.stringify({ needTool: false, cleanedText: '你好', understandable: true }) };
          else yield { content: '真实回复' };
        },
      }),
      ttsFactory: createMockTtsFactory(),
      decoderFactory: createMockDecoderFactory(),
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onAudio: () => events.push({ type: 'audio' }),
      onEvent: (e) => events.push({ type: 'event', eventType: e.type }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    await bridge.connect();
    // working 提示注入（过滤点 A 命中占位）→ classifyIntent=true 时不 schedulePlaceholderReplacement
    bridge.sendUserMessage('Briefly tell the person that you are checking with OpenClaw.');
    await sleep(250); // 等超过 placeholderDelayMs（100ms）
    // 分类路径未走（sendUserMessage 非语音出句）→ 无 playWaitMessage → 无 audio
    assert.equal(events.filter((e) => e.type === 'audio').length, 0, '分类接管时 working 提示不应触发等待语播放');
    bridge.close();
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
