// scripts/stt-pipeline-test.cjs
// 音频入→文本出链路测试（Phase 1 bridge 核心，subtask 06）
// 用法: node scripts/stt-pipeline-test.cjs
// 退出码: 0 = 全部通过, 1 = 存在失败或模型缺失
//
// 说明: scripts/ 是 CJS（.cjs），src/ 是 ESM（type:module），
// CJS 无法 require ESM，这里用动态 import 加载 src 模块，包在 async main() 里。
//
// 已确诊的 sherpa/pipeline 语义（编写依据）：
// 1. endpoint 出句后 pipeline 会调用 stt.reset()，sherpa 官方语义 reset 清空当前段
//    （Go API 文档: "GetResult(s) would also return an empty string" after reset）。
//    因此识别文本必须在 onSpeechEnd 回调里捕获——pipeline 先触发回调再 reset
//    （见 audio-pipeline.js handleSilenceWindow: speechEndCb?.() → sttRef.reset()）。
// 2. pushPcm 内 vad.process 先于 stt.feed 执行，静音尾若单块推入，
//    静音窗口触发时 recognizer 尚未收到静音，isEndpoint() 为 false，永远不出句。
//    因此静音尾必须与语音同样按 0.1s 分块喂入，让窗口周期复查 endpoint。

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const MODEL_DIR = path.join(
  __dirname,
  '..',
  'models',
  'sherpa-onnx-streaming-zipformer-bilingual-zh-en-2023-02-20'
);
const WAV_FILE = path.join(MODEL_DIR, 'test_wavs', '0.wav');

// 已知参考文本（streaming-test.cjs 无 endpoint 跑出的整段结果；本测试因 endpoint
// 分段语义不逐字断言，仅作人工核对参考）
// const KNOWN_TEXT = '昨天是 MONDAY TODAY IS LIBR THE DAY AFTER TOMORROW是星期三';

/**
 * robust WAV 解析：RIFF/WAVE 头校验 + chunk 扫描找到 fmt/data 段。
 * 不硬编码 44 字节偏移——某些 WAV 含 LIST 等扩展块，data 段位置会偏移。
 * @param {string} file
 * @returns {{pcm: Buffer, sampleRate: number, channels: number, bitsPerSample: number}}
 */
function readWavPcm16(file) {
  const buf = fs.readFileSync(file);
  assert.equal(buf.toString('ascii', 0, 4), 'RIFF', 'not a RIFF file');
  assert.equal(buf.toString('ascii', 8, 12), 'WAVE', 'not a WAVE file');

  let fmt = null;
  let data = null;
  let offset = 12;
  while (offset + 8 <= buf.length) {
    const id = buf.toString('ascii', offset, offset + 4);
    const size = buf.readUInt32LE(offset + 4);
    const bodyStart = offset + 8;
    if (id === 'fmt ') {
      fmt = {
        audioFormat: buf.readUInt16LE(bodyStart),
        channels: buf.readUInt16LE(bodyStart + 2),
        sampleRate: buf.readUInt32LE(bodyStart + 4),
        bitsPerSample: buf.readUInt16LE(bodyStart + 14),
      };
    } else if (id === 'data') {
      data = { offset: bodyStart, size };
      break; // data 段通常在末尾；找到即停
    }
    const next = bodyStart + size + (size & 1); // chunk 偶数对齐
    if (next >= buf.length) break;
    offset = next;
  }

  assert.ok(fmt, `no fmt chunk found in ${file}`);
  assert.ok(data, `no data chunk found in ${file}`);
  assert.equal(fmt.audioFormat, 1, `expected PCM (fmt 1), got ${fmt.audioFormat}`);
  assert.equal(fmt.bitsPerSample, 16, `expected 16-bit PCM, got ${fmt.bitsPerSample}`);
  assert.equal(fmt.channels, 1, `expected mono, got ${fmt.channels} channels`);

  return {
    pcm: buf.subarray(data.offset, data.offset + data.size),
    sampleRate: fmt.sampleRate,
    channels: fmt.channels,
    bitsPerSample: fmt.bitsPerSample,
  };
}

async function main() {
  if (!fs.existsSync(MODEL_DIR)) {
    console.error(`[SKIP] model directory not found: ${MODEL_DIR}`);
    console.error('  Download the sherpa-onnx streaming bilingual model into models/ first.');
    process.exit(1);
  }
  if (!fs.existsSync(WAV_FILE)) {
    console.error(`[SKIP] test wav not found: ${WAV_FILE}`);
    console.error('  Use models/sherpa-onnx-streaming-zipformer-bilingual-zh-en-2023-02-20/test_wavs/0.wav');
    console.error('  (NOT models/test_zh.wav — that file is MPEG ADTS MP3 disguised as WAV)');
    process.exit(1);
  }

  const { createStt } = await import('../src/stt/sherpa-stt.js');
  const { createPipeline } = await import('../src/bridge/audio-pipeline.js');

  const { pcm, sampleRate } = readWavPcm16(WAV_FILE);
  const audioSeconds = pcm.length / 2 / sampleRate; // PCM16 mono: 每样本 2 字节
  console.log(`audio: ${WAV_FILE}`);
  console.log(`  sampleRate=${sampleRate}, samples=${Math.floor(pcm.length / 2)}, duration=${audioSeconds.toFixed(2)}s`);

  // 模型路径缺失/模型文件不全时 createStt 会抛清晰错误 → 友好打印后 exit 1
  let stt;
  let pipeline;
  try {
    stt = createStt({ modelPath: MODEL_DIR });
    pipeline = createPipeline({ stt, inputSampleRate: 16000 });
  } catch (err) {
    console.error(`[SKIP] failed to init STT/pipeline: ${err.message}`);
    process.exit(1);
  }

  let speechStarts = 0;
  let speechEnds = 0;
  /** @type {string[]} endpoint 出句捕获的文本（reset 前 getResult 仍含本句） */
  const segments = [];
  /** @type {Error[]} */
  const errors = [];

  pipeline.onSpeechStart(() => {
    speechStarts += 1;
  });
  pipeline.onSpeechEnd(() => {
    speechEnds += 1;
    try {
      const text = stt.getResult().text;
      segments.push(text);
    } catch (err) {
      errors.push(err);
    }
  });
  pipeline.onError((err) => {
    errors.push(err);
  });

  const t0 = Date.now();

  // 分块 0.1s @16k（1600 samples = 3200 字节），完整喂完音频
  const CHUNK_BYTES = 1600 * 2;
  try {
    for (let i = 0; i < pcm.length; i += CHUNK_BYTES) {
      pipeline.pushPcm(pcm.subarray(i, i + CHUNK_BYTES));
    }
    // 追加 3s 静音触发 endpoint（1600*2*30 = 96000 字节）。
    // 必须与语音同粒度分块：单块推入时 VAD 窗口先于 feed 触发，endpoint 永不达标。
    const SILENCE_BYTES = 1600 * 2 * 30;
    const silence = Buffer.alloc(SILENCE_BYTES);
    for (let i = 0; i < silence.length; i += CHUNK_BYTES) {
      pipeline.pushPcm(silence.subarray(i, i + CHUNK_BYTES));
    }
  } finally {
    pipeline.destroy();
  }
  const wallMs = Date.now() - t0;

  // 最终 getResult：endpoint 出句后 pipeline 已 reset，sherpa 语义清空当前段，
  // 预期为空串——识别文本以 onSpeechEnd 捕获为准（此处仅信息展示）。
  const finalText = stt.getResult().text;
  const rtf = wallMs / 1000 / audioSeconds;

  const recognized = segments.filter((t) => t && t.length > 0);
  console.log('--- RESULT ---');
  if (recognized.length > 0) {
    recognized.forEach((t, i) => console.log(`  segment[${i}]: ${t}`));
  } else {
    console.log('  (no non-empty segment captured at speechEnd)');
  }
  console.log(`  final getResult().text: ${JSON.stringify(finalText)} (empty = cleared by endpoint reset, expected)`);
  console.log(`  wall=${wallMs}ms, RTF=${rtf.toFixed(3)} (${wallMs}ms / ${audioSeconds.toFixed(2)}s audio)`);
  console.log(`  speechStart=${speechStarts}, speechEnd=${speechEnds}, errors=${errors.length}`);

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

  check('recognized text is non-empty (captured at speechEnd)', () => {
    assert.ok(recognized.length >= 1, `no non-empty segment; segments=${JSON.stringify(segments)}`);
  });
  check('speechEnd >= 1 (endpoint emitted a sentence)', () => {
    assert.ok(speechEnds >= 1, `speechEnd=${speechEnds}`);
  });
  check('speechStart >= 1 (energy VAD detected speech)', () => {
    assert.ok(speechStarts >= 1, `speechStart=${speechStarts}`);
  });
  check('no pipeline errors reported', () => {
    assert.deepEqual(errors.map((e) => e.message), []);
  });

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
  console.log('PASS: 音频入→文本出链路跑通（0.wav → sherpa 文本）');
}

main().catch((err) => {
  console.error(`[ERROR] ${err && err.stack ? err.stack : err}`);
  process.exit(1);
});
