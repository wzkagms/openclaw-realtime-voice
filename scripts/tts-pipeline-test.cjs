// scripts/tts-pipeline-test.cjs
// 文本入 → 音频出链路测试（Phase 2 TTS 管线，subtask 10）
// 用法: node scripts/tts-pipeline-test.cjs
// 退出码: 0 = 全部通过, 1 = 存在失败或网络不可用
//
// 说明: scripts/ 是 CJS（.cjs），src/ 是 ESM（type:module），
// CJS 无法 require ESM，这里用动态 import 加载 src 模块，包在 async main() 里。
//
// 管线契约（编写依据，见 src/tts/*.js 与 .tmp/external-context/）：
// 1. createEdgeTts() → synthesize(text) 返回 AsyncGenerator<TTSChunk>；
//    audio 类型 chunk.data 是 mp3 Buffer；WordBoundary 类型带 text/duration/offset。
//    每个实例同一时刻只允许一个 active stream；validateText 拒绝空字符串（同步抛 TypeError）。
// 2. createDecodePipeline() → decode(edgeTtsStream) 消费整条流攒完整 mp3 再整段解码，
//    产出 { pcm16: Buffer(20ms 帧), sampleRate: 24000 }；最后一帧可更短。
//    edge-tts-universal 输出契约: audio-24khz-48kbitrate-mono-mp3 → 24kHz / mono。
// 3. WordBoundary 需单独跑一次合成收集——decode() 会消费整条 stream，无法复用。
// 4. 网络不可用（微软在线 TTS）时统一打印 [SKIP] 并 exit 1，
//    让编排层知道是环境问题而非管线 bug（acceptance: 网络失败抛含 ECONNREFUSED/ENOTFOUND 的 Error）。

const assert = require('node:assert/strict');

// 中文短句（< 50 字），真实合成不 Mock
const TEXT_1 = '你好，今天天气真好';
// reset() 复用测试第二段
const TEXT_2 = '今天真是个好日子';

// 网络/微软服务端不可用特征（Node 网络错误 + edge-tts-universal 异常关键字）。
// 命中任一即视为网络问题 → [SKIP] exit 1。
const NETWORK_ERROR_PATTERNS = [
  'ECONNREFUSED',
  'ECONNRESET',
  'ENOTFOUND',
  'ETIMEDOUT',
  'EAI_AGAIN',
  'socket hang up',
  'getaddrinfo',
  'WebSocketError',
  'NoAudioReceived',
  '403',
  'Forbidden',
];

function isNetworkError(err) {
  const msg = err && err.message ? err.message : String(err);
  return NETWORK_ERROR_PATTERNS.some((p) => msg.includes(p));
}

/** 完整消费一次 synthesize 流，返回 chunk 统计（含 WordBoundary 计数）。 */
async function collectSynthesis(tts, text) {
  const stats = { chunks: 0, audioBytes: 0, wordBoundaries: 0, sentenceBoundaries: 0 };
  for await (const chunk of tts.synthesize(text)) {
    stats.chunks += 1;
    if (chunk.type === 'audio' && Buffer.isBuffer(chunk.data)) {
      stats.audioBytes += chunk.data.length;
    } else if (chunk.type === 'WordBoundary') {
      stats.wordBoundaries += 1;
    } else if (chunk.type === 'SentenceBoundary') {
      stats.sentenceBoundaries += 1;
    }
  }
  return stats;
}

/** 执行一次合成；网络不可用统一 [SKIP] exit 1，其余错误原样抛出。 */
async function runSynthesis(tts, text) {
  try {
    return await collectSynthesis(tts, text);
  } catch (err) {
    if (isNetworkError(err)) {
      console.error(`[SKIP] edge-tts 网络不可用: ${err.message}`);
      console.error('  这是网络/微软服务端问题，不是管线 bug。联网后重跑本脚本。');
      process.exit(1);
    }
    throw err;
  }
}

/** decode 一段合成流；网络不可用统一 [SKIP] exit 1。 */
async function runDecode(pipeline, tts, text) {
  try {
    const frames = [];
    for await (const frame of pipeline.decode(tts.synthesize(text))) {
      frames.push(frame);
    }
    return frames;
  } catch (err) {
    if (isNetworkError(err)) {
      console.error(`[SKIP] edge-tts 网络不可用: ${err.message}`);
      console.error('  这是网络/微软服务端问题，不是管线 bug。联网后重跑本脚本。');
      process.exit(1);
    }
    throw err;
  }
}

async function main() {
  const { createEdgeTts } = await import('../src/tts/edge-tts.js');
  const { createDecodePipeline, TARGET_SAMPLE_RATE, FRAME_BYTES } = await import('../src/tts/decode-pipeline.js');

  // 真实网络 TTS：给足连接超时，避免断网时长时间卡住
  const tts = createEdgeTts({ connectionTimeout: 15000 });
  const pipeline = createDecodePipeline();

  // ---- 网络前置检查 + WordBoundary 采集（单独跑一次合成，decode 会消费流）----
  const wbStats = await runSynthesis(tts, TEXT_1);
  console.log(`synthesize('${TEXT_1}'): chunks=${wbStats.chunks}, audioBytes=${wbStats.audioBytes}, WordBoundary=${wbStats.wordBoundaries}`);

  // ---- 第一段：合成 + 解码 ----
  const frames1 = await runDecode(pipeline, tts, TEXT_1);
  const totalBytes1 = frames1.reduce((sum, f) => sum + f.pcm16.length, 0);
  const sampleRate1 = frames1.length > 0 ? frames1[0].sampleRate : null;
  const seconds1 = totalBytes1 / (TARGET_SAMPLE_RATE * 2);
  console.log(`decode('${TEXT_1}'): frames=${frames1.length}, totalBytes=${totalBytes1}, sampleRate=${sampleRate1}, duration≈${seconds1.toFixed(3)}s`);

  // ---- 断言（node:assert 零依赖）----
  let passed = 0;
  let failed = 0;
  const failures = [];

  function check(name, fn) {
    try {
      fn();
      passed += 1;
      console.log(`PASS: ${name}`);
    } catch (err) {
      failed += 1;
      failures.push({ name, err });
      console.log(`FAIL: ${name} — ${err.message}`);
    }
  }

  check('pcm16 buffer 非空（Buffer.length > 0）', () => {
    assert.ok(totalBytes1 > 0, `totalBytes=${totalBytes1}`);
  });
  check('sampleRate === 24000（edge-tts 契约）', () => {
    assert.equal(sampleRate1, TARGET_SAMPLE_RATE, `sampleRate=${sampleRate1}`);
  });
  check('音频时长 ≥ 0.3s（totalBytes / (24000 × 2)）', () => {
    assert.ok(seconds1 >= 0.3, `duration=${seconds1.toFixed(3)}s < 0.3s`);
  });
  check('20ms 帧输出：每帧 960 bytes（最后一帧可更短）', () => {
    assert.ok(frames1.length > 0, 'no frames decoded');
    frames1.slice(0, -1).forEach((f, i) => {
      assert.equal(f.pcm16.length, FRAME_BYTES, `frame[${i}] length=${f.pcm16.length}, expected ${FRAME_BYTES}`);
    });
  });
  check('WordBoundary 事件 ≥ 1（单独遍历一次合成流）', () => {
    assert.ok(wbStats.wordBoundaries >= 1, `wordBoundaries=${wbStats.wordBoundaries}`);
  });

  // ---- 第二段：reset() 后复用同一管线实例 ----
  await pipeline.reset();
  const frames2 = await runDecode(pipeline, tts, TEXT_2);
  const totalBytes2 = frames2.reduce((sum, f) => sum + f.pcm16.length, 0);
  console.log(`reset() + decode('${TEXT_2}'): frames=${frames2.length}, totalBytes=${totalBytes2}`);

  check('decodePipeline.reset() 后可复用解码第二段（pcm16 非空）', () => {
    assert.ok(totalBytes2 > 0, `totalBytes=${totalBytes2}`);
  });

  // ---- 入参校验：空字符串拒绝（synthesize 同步抛 TypeError）----
  check("synthesize('') 抛 TypeError", () => {
    assert.throws(() => tts.synthesize(''), TypeError);
  });

  pipeline.free();

  console.log(`\n${passed} passed, ${failed} failed`);
  if (failed > 0) {
    console.log('\nFailed cases:');
    for (const { name, err } of failures) {
      console.log(`  [${name}]`);
      console.log(`    message: ${err.message}`);
    }
    process.exitCode = 1;
    return;
  }
  console.log('PASS: 文本入→音频出链路跑通（edge-tts → mpg123 → 20ms PCM16 帧）');
}

main().catch((err) => {
  console.error(`[ERROR] ${err && err.stack ? err.stack : err}`);
  process.exit(1);
});
