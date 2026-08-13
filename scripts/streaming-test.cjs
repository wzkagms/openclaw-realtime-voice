// sherpa-onnx-node 流式识别验证（Windows x64）
// 用法: node scripts/streaming-test.cjs
const fs = require('fs');
const path = require('path');
const { OnlineRecognizer } = require('sherpa-onnx-node');

const modelDir = path.join(__dirname, '..', 'models', 'sherpa-onnx-streaming-zipformer-bilingual-zh-en-2023-02-20');

const recognizer = new OnlineRecognizer({
  featConfig: { sampleRate: 16000, featureDim: 80 },
  modelConfig: {
    transducer: {
      encoder: path.join(modelDir, 'encoder-epoch-99-avg-1.int8.onnx'),
      decoder: path.join(modelDir, 'decoder-epoch-99-avg-1.int8.onnx'),
      joiner: path.join(modelDir, 'joiner-epoch-99-avg-1.int8.onnx'),
    },
    tokens: path.join(modelDir, 'tokens.txt'),
    numThreads: 2,
    provider: 'cpu',
    debug: 0,
  },
  enableEndpointDetection: true,
  enableEndpoint: true,
  rule1MinTrailingSilence: 2.4,
  rule2MinTrailingSilence: 1.2,
  rule3MinUtteranceLength: 20,
});

// 解析 16-bit PCM mono WAV（跳过 44 字节头）
function readWav(file) {
  const buf = fs.readFileSync(file);
  const sampleRate = buf.readUInt32LE(24);
  const bits = buf.readUInt16LE(34);
  const channels = buf.readUInt16LE(22);
  const dataOffset = 44; // 简化：标准头
  const bytesPerSample = bits / 8;
  const n = (buf.length - dataOffset) / (bytesPerSample * channels);
  const samples = new Float32Array(n);
  for (let i = 0; i < n; i++) {
    const offset = dataOffset + i * bytesPerSample * channels;
    let v;
    if (bits === 16) v = buf.readInt16LE(offset);
    else if (bits === 32) v = buf.readInt32LE(offset) / 65536;
    else v = (buf[offset] - 128) * 256;
    samples[i] = v / 32768;
  }
  return { samples, sampleRate };
}

const wavFile = process.argv[2] || path.join(modelDir, 'test_wavs', '0.wav');
const { samples, sampleRate } = readWav(wavFile);
console.log(`audio: ${wavFile}`);
console.log(`  sampleRate=${sampleRate}, samples=${samples.length} (${(samples.length / sampleRate).toFixed(2)}s)`);

const t0 = Date.now();
const stream = recognizer.createStream();
const chunk = 1600; // 0.1s @16k
for (let i = 0; i < samples.length; i += chunk) {
  const end = Math.min(i + chunk, samples.length);
  stream.acceptWaveform({ sampleRate: 16000, samples: samples.subarray(i, end) });
  while (recognizer.isReady(stream)) {
    recognizer.decode(stream);
  }
  const text = recognizer.getResult(stream).text;
  if (text) process.stdout.write(`\r[${(i / sampleRate).toFixed(1)}s] ${text}   `);
}
process.stdout.write('\n');
const result = recognizer.getResult(stream).text;
const ms = Date.now() - t0;
console.log('--- FINAL RESULT ---');
console.log(JSON.stringify(result));
console.log(`RTF: ${(ms / 1000 / (samples.length / sampleRate)).toFixed(3)} (${ms}ms wall for ${(samples.length / sampleRate).toFixed(2)}s audio)`);
stream.free();
recognizer.free();
