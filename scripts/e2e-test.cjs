// scripts/e2e-test.cjs
// Phase 3 端到端验证（高保真模拟 gateway-relay）：
//   test_wavs 16k wav → LinearResampler 重采样 24k（模拟 gateway 把浏览器 48k 转成声明 24k）
//   → bridge.sendAudio 分块喂入（20ms 块 @24k）→ 补 3s 静音触发 endpoint
//   → sherpa STT 识别 → mock LLM(SSE) 回复 → edge-tts 合成 → mpg123 解码 → onAudio 帧
// 证据输出：状态流转日志 + 识别文本 + LLM 回复 + 音频帧数/总时长 + 采样率推算
//   （getAudioStats 审计：totalBytes / 2 / durationSeconds ≈ 24000）
// 用法: node scripts/e2e-test.cjs [wavPath]
// 退出码: 0 = 全部通过, 1 = 存在失败
//
// 说明:
//   - scripts/ 是 CJS（.cjs），src/ 是 ESM（type:module），动态 import 加载。
//   - edge-tts 走真实微软服务（需联网）：断网时链路失败，属环境问题，如实报告。
//   - sherpa 模型（models/sherpa-...）已存在：connect() 强依赖（不再降级跳过）。

const http = require('node:http');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { createRequire } = require('node:module');

const require_ = createRequire(require.resolve('../package.json'));
const { LinearResampler } = require_('sherpa-onnx-node');

// ---------- 工具 ----------

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * 解析标准 RIFF/WAVE 16-bit PCM mono wav（test_wavs 格式）：
 * 定位 fmt（audioFormat/channels/sampleRate/bitsPerSample）+ data chunk。
 * @param {string} filePath
 * @returns {{sampleRate: number, channels: number, bitsPerSample: number, pcm16: Buffer, durationSec: number}}
 */
function parseWav(filePath) {
  const buf = fs.readFileSync(filePath);
  if (buf.toString('ascii', 0, 4) !== 'RIFF' || buf.toString('ascii', 8, 12) !== 'WAVE') {
    throw new Error(`parseWav: ${filePath} is not a RIFF/WAVE file`);
  }
  let offset = 12;
  let fmt = null;
  let data = null;
  while (offset + 8 <= buf.length) {
    const id = buf.toString('ascii', offset, offset + 4);
    const size = buf.readUInt32LE(offset + 4);
    if (id === 'fmt ') {
      fmt = {
        audioFormat: buf.readUInt16LE(offset + 8),
        channels: buf.readUInt16LE(offset + 10),
        sampleRate: buf.readUInt32LE(offset + 12),
        bitsPerSample: buf.readUInt16LE(offset + 22),
      };
    } else if (id === 'data') {
      data = buf.subarray(offset + 8, offset + 8 + size);
      break;
    }
    offset += 8 + size + (size % 2);
  }
  if (!fmt || !data) throw new Error(`parseWav: ${filePath} missing fmt/data chunk`);
  if (fmt.audioFormat !== 1 || fmt.bitsPerSample !== 16) {
    throw new Error(`parseWav: ${filePath} must be PCM16 (got fmt=${fmt.audioFormat}, bits=${fmt.bitsPerSample})`);
  }
  const durationSec = data.length / (fmt.sampleRate * fmt.channels * 2);
  return { ...fmt, pcm16: Buffer.from(data), durationSec };
}

/** PCM16 LE Buffer → Float32Array（归一化 [-1,1]）。 */
function pcm16ToFloat32(buffer) {
  const count = Math.floor(buffer.length / 2);
  const int16 = new Int16Array(buffer.buffer, buffer.byteOffset, count);
  const samples = new Float32Array(count);
  for (let i = 0; i < count; i++) samples[i] = int16[i] / 32768;
  return samples;
}

/** Float32Array（-1..1）→ PCM16 LE Buffer。 */
function float32ToPcm16(samples) {
  const int16 = new Int16Array(samples.length);
  for (let i = 0; i < samples.length; i++) {
    const v = Math.round(samples[i] * 32767);
    int16[i] = v < -32768 ? -32768 : v > 32767 ? 32767 : v;
  }
  return Buffer.from(int16.buffer, int16.byteOffset, int16.byteLength);
}

/**
 * mock OpenAI 兼容 LLM server（SSE 流式，逐字符返回 replyText + [DONE]）。
 * 复用 bridge-llm-tts-test.cjs 的已验证模式。
 * @param {{replyText?: string, chunkDelayMs?: number}} [options]
 */
function createMockLlmServer({ replyText = '好的，这是端到端语音测试回复。', chunkDelayMs = 30 } = {}) {
  const server = http.createServer((req, res) => {
    if (req.method !== 'POST' || req.url !== '/chat/completions') {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'not found' }));
      return;
    }
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
    });
    const chars = Array.from(replyText);
    let i = 0;
    const sendNext = () => {
      if (i >= chars.length) {
        res.write('data: [DONE]\n\n');
        res.end();
        return;
      }
      const payload = {
        id: 'chatcmpl-e2e',
        object: 'chat.completion.chunk',
        created: Date.now(),
        model: 'test-model',
        choices: [{ index: 0, delta: { content: chars[i++] }, finish_reason: null }],
      };
      res.write(`data: ${JSON.stringify(payload)}\n\n`);
      setTimeout(sendNext, chunkDelayMs);
    };
    sendNext();
  });
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      const port = server.address().port;
      resolve({
        server,
        port,
        baseUrl: `http://127.0.0.1:${port}`,
        close: () => new Promise((r) => server.close(r)),
      });
    });
  });
}

/**
 * 轮询等待谓词为真。
 * @param {() => boolean} predicate
 * @param {number} [timeoutMs]
 * @param {number} [intervalMs]
 */
async function waitFor(predicate, timeoutMs = 90000, intervalMs = 50) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (predicate()) return true;
    if (Date.now() >= deadline) return false;
    await sleep(intervalMs);
  }
}

// ---------- 主流程 ----------

async function main() {
  const wavPath = process.argv[2] ?? path.join(
    __dirname,
    '..',
    'models',
    'sherpa-onnx-streaming-zipformer-bilingual-zh-en-2023-02-20',
    'test_wavs',
    '0.wav',
  );

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

  const llmServer = await createMockLlmServer();
  try {
    // 1. 解析 + 重采样 16k → 24k（模拟 gateway-relay：浏览器 48k → resamplePcm → 声明 24k）
    const wav = parseWav(wavPath);
    if (wav.sampleRate !== 16000) {
      throw new Error(`e2e: expected 16k input wav, got ${wav.sampleRate} (${wavPath})`);
    }
    const resampler = new LinearResampler(16000, 24000);
    const resampled24k = resampler.resample(pcm16ToFloat32(wav.pcm16));
    const inputPcm24k = float32ToPcm16(resampled24k);
    const silence3s = Buffer.alloc(24000 * 2 * 3); // 3s 静音 @24k PCM16，触发 sherpa endpoint

    console.log(`[e2e] input wav: ${path.basename(wavPath)}`);
    console.log(`[e2e] wav: ${wav.sampleRate}Hz mono, ${wav.durationSec.toFixed(2)}s`);
    console.log(`[e2e] resampled to 24k: ${inputPcm24k.length} bytes (${(inputPcm24k.length / 2 / 24000).toFixed(2)}s)`);

    // 2. 创建 bridge（providerConfig → mock LLM；inputSampleRate 24000 与声明格式一致）
    const { createBridgeRuntime } = await import('../src/bridge/bridge-runtime.js');
    const transcripts = [];
    const audioFrames = [];
    const marks = [];
    const errors = [];
    let clearCount = 0;
    const stateSeen = [];
    const bridge = createBridgeRuntime({
      providerConfig: { baseUrl: llmServer.baseUrl, apiKey: 'e2e-test-key', model: 'test-model' },
      inputSampleRate: 24000,
      ackEnabled: false, // e2e 聚焦语音→回复链路，关闭 ack（出句「嗯」会干扰 transcript 断言）
      onTranscript: (role, text, isFinal) => transcripts.push({ role, text, isFinal }),
      onAudio: (buf) => audioFrames.push(buf),
      onMark: (name) => marks.push(name),
      onClearAudio: () => { clearCount += 1; },
      onError: (err) => errors.push(err),
    });
    const pollState = () => {
      const s = bridge.getState();
      if (!stateSeen.includes(s)) stateSeen.push(s);
      return s;
    };

    // 3. connect（sherpa 模型存在）
    await bridge.connect();
    check('connect() → isConnected() === true', () => assert.equal(bridge.isConnected(), true));
    check('connect() → state IDLE', () => assert.equal(pollState(), 'IDLE'));

    // 4. 分块喂入 24k PCM（20ms 块 = 960 字节 @24k，模拟 gateway-relay 实时流节奏）：
    //    每块 sleep 20ms 真实时间流速——保证 LISTENING 态可被轮询观测。
    //    流完成等待与喂帧并行启动（waitFor 在喂帧后启动会错过喂帧期间的
    //    THINKING/SPEAKING 态，且 TTS 提前完成时剩余静音帧会把 IDLE 拉回 LISTENING）。
    const frameBytes = 24000 * 20 * 2 / 1000; // 960
    const flowDonePromise = waitFor(() => pollState() === 'IDLE' && stateSeen.length > 1, 90000);
    let fedBytes = 0;
    for (let offset = 0; offset < inputPcm24k.length; offset += frameBytes) {
      bridge.sendAudio(inputPcm24k.subarray(offset, offset + frameBytes));
      fedBytes += Math.min(frameBytes, inputPcm24k.length - offset);
      await sleep(20);
    }
    // 静音尾巴分块喂入（VAD 静音窗口 + sherpa endpoint 出句）：
    // 一旦状态离开 LISTENING（endpoint 已触发、进入 THINKING）立即停止喂帧，
    // 避免 TTS 完成后剩余静音帧把 IDLE 拉回 LISTENING（最终状态断言失真）。
    for (let offset = 0; offset < silence3s.length && bridge.getState() === 'LISTENING'; offset += frameBytes) {
      bridge.sendAudio(silence3s.subarray(offset, offset + frameBytes));
      await sleep(20);
    }
    console.log(`[e2e] fed ${fedBytes} bytes audio + silence until endpoint (20ms chunks)`);

    // 5. 等待链路完成：LISTENING → (RECOGNIZING) → THINKING → SPEAKING → IDLE
    const done = await flowDonePromise;
    check('e2e flow completes (state back to IDLE)', () =>
      assert.ok(done, `stateSeen=${JSON.stringify(stateSeen)}`),
    );

    // 6. 状态流转证据
    check('state flow includes LISTENING', () => assert.ok(stateSeen.includes('LISTENING'), JSON.stringify(stateSeen)));
    check('state flow includes THINKING', () => assert.ok(stateSeen.includes('THINKING'), JSON.stringify(stateSeen)));
    check('state flow includes SPEAKING', () => assert.ok(stateSeen.includes('SPEAKING'), JSON.stringify(stateSeen)));
    check('state flow ends at IDLE', () => assert.equal(pollState(), 'IDLE'));

    // 7. STT 识别证据（RECOGNIZING 瞬态由 user transcript 证明）
    const userFinal = transcripts.find((t) => t.role === 'user' && t.isFinal === true);
    check('STT recognized non-empty user text', () => {
      assert.ok(userFinal && userFinal.text.trim().length > 0, `transcripts=${JSON.stringify(transcripts)}`);
    });
    console.log(`[e2e] STT text: "${userFinal ? userFinal.text : '(none)'}"`);

    // 8. LLM 回复证据
    const assistantFinal = transcripts.find((t) => t.role === 'assistant' && t.isFinal === true);
    check('LLM replied non-empty assistant text', () => {
      assert.ok(assistantFinal && assistantFinal.text.trim().length > 0, `transcripts=${JSON.stringify(transcripts)}`);
    });
    console.log(`[e2e] LLM reply: "${assistantFinal ? assistantFinal.text : '(none)'}"`);

    // 9. TTS 输出证据：帧数、960 字节帧、总时长（帧 × 20ms）
    check('onAudio received at least 1 pcm16 frame', () => assert.ok(audioFrames.length >= 1, `frames=${audioFrames.length}`));
    check('onAudio has 960-byte frames (20ms @ 24k)', () => {
      assert.ok(audioFrames.some((f) => f.length === 960), `lengths=${audioFrames.slice(0, 10).map((f) => f.length).join(',')}...`);
    });
    const totalAudioMs = audioFrames.length * 20;
    check('output audio duration >= 1s', () => assert.ok(totalAudioMs >= 1000, `totalAudioMs=${totalAudioMs}`));
    console.log(`[e2e] output audio: ${audioFrames.length} frames, ${totalAudioMs}ms (${(totalAudioMs / 1000).toFixed(2)}s @ 24k)`);

    // 10. mark 证据（response-start / response-end 契约）
    check('marks include response-start', () => assert.ok(marks.includes('response-start'), JSON.stringify(marks)));
    check('marks include response-end', () => assert.ok(marks.includes('response-end'), JSON.stringify(marks)));

    // 11. 采样率审计：getAudioStats 字节 → 推算实际采样率。
    //     实际喂入量 = wav 全量 + 到 endpoint 触发为止的静音（提前停止，动态量）。
    //     实际时长由 chunk 结构推算：满块（960 字节 = 20ms @24k）+ 尾块时长。
    const stats = bridge.getAudioStats();
    const fullChunks = stats.chunkCount - 1;
    const lastBytes = stats.totalBytes - fullChunks * frameBytes;
    const actualDurationSec = fullChunks * 0.02 + (lastBytes / frameBytes) * 0.02;
    const inferredSampleRate = stats.totalBytes / 2 / actualDurationSec;
    check('audio stats: chunkCount > 0', () => assert.ok(stats.chunkCount > 0, JSON.stringify(stats)));
    check('audio stats: audio fully fed (totalBytes >= wav bytes)', () =>
      assert.ok(stats.totalBytes >= inputPcm24k.length, `stats=${JSON.stringify(stats)}`),
    );
    check('audio stats: totalBytes <= wav + full silence (endpoint stopped early)', () =>
      assert.ok(stats.totalBytes <= fedBytes + silence3s.length, `stats=${JSON.stringify(stats)}`),
    );
    check('inferred input sample rate ≈ 24000 (±1)', () => {
      assert.ok(Math.abs(inferredSampleRate - 24000) < 1, `inferred=${inferredSampleRate.toFixed(2)}`);
    });
    console.log(
      `[e2e] sample-rate audit: ${stats.totalBytes} bytes over ${actualDurationSec.toFixed(2)}s = ${inferredSampleRate.toFixed(2)} Hz (declared 24000)`,
    );

    // 12. 无运行时错误
    check('no runtime errors', () => assert.deepEqual(errors.map((e) => e.message), []));
    check('no spurious clear (barge-in)', () => assert.equal(clearCount, 0, `clearCount=${clearCount}`));

    // ---------- 证据报告 ----------
    console.log('\n===== E2E evidence =====');
    console.log(`input:  ${path.basename(wavPath)} (${wav.sampleRate}Hz ${wav.durationSec.toFixed(2)}s -> 24k ${(inputPcm24k.length / 2 / 24000).toFixed(2)}s)`);
    console.log(`states: ${stateSeen.join(' -> ')}`);
    console.log(`STT:    ${userFinal ? userFinal.text : '(none)'}`);
    console.log(`LLM:    ${assistantFinal ? assistantFinal.text : '(none)'}`);
    console.log(`TTS:    ${audioFrames.length} frames, ${totalAudioMs}ms @ 24k`);
    console.log(`marks:  ${JSON.stringify(marks)}`);
    console.log(`audit:  ${stats.totalBytes} bytes, inferred ${inferredSampleRate.toFixed(1)}Hz (declared 24000)`);
    console.log(`errors: ${errors.length === 0 ? 'none' : errors.map((e) => e.message).join(' | ')}`);
    console.log(`========================\n`);

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
    console.log('PASS: e2e voice loop 跑通（音频入 -> sherpa STT -> mock LLM -> edge-tts -> onAudio）');
  } finally {
    try {
      bridge?.close();
    } catch {
      // no-op
    }
    try {
      await llmServer.close();
    } catch {
      // no-op
    }
  }
}

main().catch((err) => {
  console.error(`[ERROR] ${err && err.stack ? err.stack : err}`);
  process.exit(1);
});
