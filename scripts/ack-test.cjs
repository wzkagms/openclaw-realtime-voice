// scripts/ack-test.cjs
// Phase 4 ack 两层反馈验证（澜影拍板，砍掉「嗯」）：
//   1) 连接就绪播「在的」（readyText）
//   2) 真实回复正常播放（ack 不干扰 sendUserMessage 链路）
//   3) 语音出句路径正常（「在的」播放后识别不受影响）
// 用法: node scripts/ack-test.cjs
// 退出码: 0 = 全部通过, 1 = 存在失败

const assert = require('node:assert/strict');
const path = require('node:path');
const fs = require('node:fs');

const MODEL_DIR = path.join(
  __dirname, '..', 'models', 'sherpa-onnx-streaming-zipformer-bilingual-zh-en-2023-02-20',
);
if (!fs.existsSync(path.join(MODEL_DIR, 'encoder-epoch-99-avg-1.onnx'))) {
  console.error('[SKIP] sherpa model missing — ack-test requires local int8 model');
  process.exit(0);
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function waitFor(predicate, timeoutMs = 5000, intervalMs = 20) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (predicate()) return true;
    if (Date.now() >= deadline) return false;
    await sleep(intervalMs);
  }
}

/** 共享实例 mock TTS（记录合成文本）。 */
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

function createMockDecoderFactory() {
  return () => ({
    reset: async () => {},
    decode: async function* (stream) {
      for await (const chunk of stream) {
        if (chunk?.type === 'audio') yield { pcm16: Buffer.from(chunk.data), sampleRate: 24000 };
      }
    },
    free: () => {},
  });
}

/** 分类器 mock：needTool=false + cleanedText 原样（不干扰 ack 测试）。 */
function createClassifierMock() {
  return () => ({
    streamChat: async function* (messages) {
      const user = messages.find((m) => m.role === 'user')?.content ?? '';
      yield {
        content: JSON.stringify({ needTool: false, cleanedText: user, understandable: true }),
      };
    },
  });
}

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
  const { pcm } = readWavPcm16(WAV);
  const { createBridgeRuntime } = await import('../src/bridge/bridge-runtime.js');

  // ---------- 测试 1：连接就绪播「在的」 ----------
  await test('连接就绪 → 播「在的」', async () => {
    const events = [];
    const ttsFactory = createMockTtsFactory();
    const bridge = createBridgeRuntime({
      providerConfig: { baseUrl: 'http://mock', apiKey: 'k', model: 'm' },
      autoRespondToAudio: false,
      inputSampleRate: 16000,
      llmFactory: createClassifierMock(),
      ttsFactory,
      decoderFactory: createMockDecoderFactory(),
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onAudio: () => events.push({ type: 'audio' }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    await bridge.connect();
    // warmup + 300ms 延迟后播「在的」
    const played = await waitFor(() => events.filter((e) => e.type === 'audio').length >= 1, 4000);
    assert.ok(played, 'connect 后应播「在的」');
    const tts = ttsFactory();
    assert.ok(tts.getCalls().some((c) => c.includes('在的')), `应预合成并播放「在的」，calls=${JSON.stringify(tts.getCalls())}`);
    bridge.close();
  });

  // ---------- 测试 2：真实回复正常播放（ack 不干扰主链路） ----------
  await test('真实回复正常播放（ack 不干扰 sendUserMessage 链路）', async () => {
    const events = [];
    const ttsFactory = createMockTtsFactory();
    const bridge = createBridgeRuntime({
      providerConfig: { baseUrl: 'http://mock', apiKey: 'k', model: 'm' },
      ackEnabled: false, // 聚焦 sendUserMessage 链路，关闭「在的」
      llmFactory: () => ({
        streamChat: async function* () {
          yield { content: '真实回复内容' };
        },
      }),
      ttsFactory,
      decoderFactory: createMockDecoderFactory(),
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onAudio: () => events.push({ type: 'audio' }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    await bridge.connect();
    bridge.sendUserMessage('测试');
    const played = await waitFor(() => events.filter((e) => e.type === 'audio').length >= 1, 4000);
    assert.ok(played, '真实回复应播放');
    bridge.close();
  });

  // ---------- 测试 3：语音出句路径正常（ackEnabled=true 时「在的」不干扰语音链路） ----------
  await test('语音出句路径正常（「在的」播放后语音识别不受影响）', async () => {
    const events = [];
    const ttsFactory = createMockTtsFactory();
    const bridge = createBridgeRuntime({
      providerConfig: { baseUrl: 'http://mock', apiKey: 'k', model: 'm' },
      autoRespondToAudio: false,
      inputSampleRate: 16000,
      llmFactory: createClassifierMock(),
      ttsFactory,
      decoderFactory: createMockDecoderFactory(),
      onTranscript: (role, text, final) => events.push({ type: 'transcript', role, text, final }),
      onAudio: () => events.push({ type: 'audio' }),
      onError: (e) => events.push({ type: 'error', message: e.message }),
    });
    await bridge.connect();
    await sleep(400); // 等「在的」播放（300ms 延迟）
    // 喂 wav 触发语音出句 → 应正常上报 user transcript
    const CHUNK = 640;
    for (let i = 0; i < pcm.length; i += CHUNK) {
      bridge.sendAudio(pcm.subarray(i, i + CHUNK));
      await sleep(3);
    }
    const silence = Buffer.alloc(CHUNK * 160);
    for (let i = 0; i < silence.length; i += CHUNK) {
      bridge.sendAudio(silence.subarray(i, i + CHUNK));
      await sleep(2);
    }
    const reported = await waitFor(() => events.some((e) => e.type === 'transcript' && e.role === 'user' && e.final), 4000);
    assert.ok(reported, '语音出句应正常上报（「在的」播放后识别不受影响）');
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
