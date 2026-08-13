// scripts/bridge-llm-tts-test.cjs
// bridge LLM+TTS 全链路测试（Phase 2，subtask 11）
// 用法: node scripts/bridge-llm-tts-test.cjs
// 退出码: 0 = 全部通过, 1 = 存在失败
//
// 被测链路: sendUserMessage(text)
//   → mock LLM (SSE /chat/completions) → 累积回复
//   → edge-tts 合成（真实微软服务，需联网）→ mpg123 解码
//   → 20ms pcm16 帧（960 bytes @24kHz）→ onAudio
//
// 说明:
//   - scripts/ 是 CJS（.cjs），src/ 是 ESM（type:module），动态 import 加载 bridge-runtime。
//   - sendUserMessage 不依赖 connect()/sherpa 模型：runLlmTtsFlow 只走 getLlm/getTts/getDecoder
//     惰性初始化，不碰 stt/pipeline（代码审查确认）。connect() 仅在 sherpa 模型存在时验证
//     isConnected()，失败则降级跳过（避免测试强依赖模型下载）。
//   - mock LLM server 用 node:http 动态端口；请求头 X-Fail: 1 → 500（另起 fail server 专测错误链路）。
//   - edge-tts 走真实网络：断网时正常链路/onAudio 断言会失败，属环境问题，如实报告。

const http = require('node:http');
const assert = require('node:assert/strict');

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * mock OpenAI 兼容 LLM server（SSE 流式）。
 * 正常模式: POST /chat/completions → SSE 逐字符返回 replyText + [DONE]。
 *           chunk 间隔 chunkDelayMs（默认 60ms），保证 THINKING 状态可被轮询观测、
 *           barge-in 时机可靠（barge-in 时 LLM 流仍在进行）。
 * fail 模式: 任何请求返回 500（错误链路用）。
 * 兼容 X-Fail: 1 请求头 → 500（客户端未来若支持可直用；当前 openai-client 不发该头）。
 * @param {{fail?: boolean, chunkDelayMs?: number, replyText?: string}} [options]
 * @returns {Promise<{server: import('node:http').Server, port: number, baseUrl: string, close: () => Promise<void>}>}
 */
function createMockLlmServer({ fail = false, chunkDelayMs = 60, replyText = '你好，这是测试回复' } = {}) {
  const server = http.createServer((req, res) => {
    if (req.method !== 'POST' || req.url !== '/chat/completions') {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'not found' }));
      return;
    }
    if (fail || req.headers['x-fail'] === '1') {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: { message: 'mock LLM server error', type: 'server_error' } }));
      return;
    }
    // SSE：按字符切 chunk，逐行 data: + 空行，结束 [DONE]（对齐 openai-client 的 SSE 解析）
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
        id: 'chatcmpl-mock',
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
 * @returns {Promise<boolean>}
 */
async function waitFor(predicate, timeoutMs = 60000, intervalMs = 50) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (predicate()) return true;
    if (Date.now() >= deadline) return false;
    await sleep(intervalMs);
  }
}

async function main() {
  const okServer = await createMockLlmServer();
  const failServer = await createMockLlmServer({ fail: true });

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

  try {
    const { createBridgeRuntime } = await import('../src/bridge/bridge-runtime.js');

    // ---------- bridge1：正常链路 + barge-in + close ----------
    const transcripts = [];
    const audioFrames = [];
    const errors1 = [];
    const clears = [];
    let readyCount = 0;
    let closeCount = 0;
    const stateSeen = [];
    const bridge1 = createBridgeRuntime({
      providerConfig: { baseUrl: okServer.baseUrl, apiKey: 'test-key', model: 'test-model' },
      ackEnabled: false, // 本测试聚焦 sendUserMessage 链路，关闭 ack（避免 connect「在的」真实合成干扰）
      onTranscript: (role, text, isFinal) => transcripts.push({ role, text, isFinal }),
      onAudio: (buf) => audioFrames.push(buf),
      onError: (err) => errors1.push(err),
      onClearAudio: (reason) => clears.push(reason),
      onReady: () => { readyCount += 1; },
      onClose: () => { closeCount += 1; },
    });
    const pollState = () => {
      const s = bridge1.getState();
      if (!stateSeen.includes(s)) stateSeen.push(s);
      return s;
    };

    check('initial state is IDLE', () => assert.equal(bridge1.getState(), 'IDLE'));

    // connect()：sherpa 模型存在则验证 isConnected/onReady；模型缺失时降级跳过，
    // 后续 sendUserMessage 链路照常（sendUserMessage 不依赖初始化，见文件头说明）。
    let connectOk = false;
    try {
      await bridge1.connect();
      connectOk = true;
    } catch (err) {
      console.warn(`[WARN] bridge.connect() failed (${err.message}); model may be missing — continuing sendUserMessage-only path`);
    }
    if (connectOk) {
      check('connect() → isConnected() === true (sherpa model present)', () => assert.equal(bridge1.isConnected(), true));
      check('connect() → onReady fired', () => assert.equal(readyCount, 1));
    }

    // ---- 正常链路：sendUserMessage → LLM → TTS → onAudio，状态回 IDLE ----
    bridge1.sendUserMessage('你好');
    const flowDone = await waitFor(() => pollState() === 'IDLE' && stateSeen.length > 1, 60000);
    check('normal flow completes (state back to IDLE)', () => assert.ok(flowDone, `stateSeen=${JSON.stringify(stateSeen)}`));
    check('onTranscript user final (isFinal=true, text=你好)', () => {
      assert.ok(
        transcripts.some((t) => t.role === 'user' && t.isFinal === true && t.text === '你好'),
        `transcripts=${JSON.stringify(transcripts)}`,
      );
    });
    check('onTranscript assistant final (isFinal=true, includes reply text)', () => {
      assert.ok(
        transcripts.some((t) => t.role === 'assistant' && t.isFinal === true && t.text.includes('测试回复')),
        `transcripts=${JSON.stringify(transcripts)}`,
      );
    });
    check('onAudio received at least 1 pcm16 frame', () => {
      assert.ok(audioFrames.length >= 1, `audioFrames=${audioFrames.length}`);
    });
    check('onAudio has a 960-byte frame (20ms @ 24kHz)', () => {
      assert.ok(audioFrames.some((f) => f.length === 960), `frame lengths=${audioFrames.map((f) => f.length).join(',')}`);
    });
    check('state flow observed THINKING → SPEAKING → IDLE', () => {
      assert.ok(stateSeen.includes('THINKING'), `stateSeen=${JSON.stringify(stateSeen)}`);
      assert.ok(stateSeen.includes('SPEAKING'), `stateSeen=${JSON.stringify(stateSeen)}`);
      assert.equal(pollState(), 'IDLE');
    });
    if (connectOk) {
      check('no runtime errors on happy path', () => assert.deepEqual(errors1.map((e) => e.message), []));
    }

    // ---- barge-in：LLM 流进行中打断 → onClearAudio + LISTENING ----
    bridge1.sendUserMessage('你好');
    await sleep(100); // 保证 mock SSE 流仍在进行（LLM 未完成，state 为 THINKING）
    bridge1.handleBargeIn();
    check('handleBargeIn → onClearAudio fired', () => {
      assert.ok(clears.length >= 1, `clears=${JSON.stringify(clears)}`);
    });
    check('handleBargeIn → state is LISTENING', () => assert.equal(bridge1.getState(), 'LISTENING'));

    // ---- close：isConnected false + onClose ----
    bridge1.close();
    check('close() → isConnected() === false', () => assert.equal(bridge1.isConnected(), false));
    check('close() → onClose fired', () => assert.equal(closeCount, 1));

    // ---------- bridge2：错误链路（fail server 500） ----------
    const errors2 = [];
    const bridge2 = createBridgeRuntime({
      providerConfig: { baseUrl: failServer.baseUrl, apiKey: 'test-key', model: 'test-model' },
      onError: (err) => errors2.push(err),
    });
    bridge2.sendUserMessage('触发错误');
    const errOk = await waitFor(() => errors2.length >= 1, 15000);
    check('LLM HTTP 500 → onError fired', () => {
      assert.ok(errOk, `errors=${errors2.map((e) => e.message).join(' | ')}`);
    });
    check('onError message includes HTTP 500', () => {
      assert.ok(errors2[0] && errors2[0].message.includes('500'), `message=${errors2[0] && errors2[0].message}`);
    });
    check('error path returns state to IDLE', () => assert.equal(bridge2.getState(), 'IDLE'));
    bridge2.close();

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
    console.log('PASS: bridge LLM+TTS 全链路跑通（sendUserMessage → mock LLM → edge-tts → onAudio）');
  } finally {
    try {
      await okServer.close();
    } catch {
      // no-op
    }
    try {
      await failServer.close();
    } catch {
      // no-op
    }
  }
}

main().catch((err) => {
  console.error(`[ERROR] ${err && err.stack ? err.stack : err}`);
  process.exit(1);
});
