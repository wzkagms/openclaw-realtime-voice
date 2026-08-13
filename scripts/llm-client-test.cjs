// scripts/llm-client-test.cjs
// LLM 客户端测试（Phase 2 核心，subtask 09）：mock SSE server 验证 src/llm/openai-client.js
// 用法: node scripts/llm-client-test.cjs
// 退出码: 0 = 全部通过, 1 = 存在失败
//
// 说明: scripts/ 是 CJS（.cjs），src/ 是 ESM（type:module），
// CJS 无法 require ESM，这里用动态 import 加载 src 模块，包在 async main() 里。
//
// 场景开关（streamChat 不暴露自定义请求头，headers 由 createOpenAiClient 内部构造，
// 因此用 apiKey 即 Authorization Bearer 值触发 mock 场景，顺带验证鉴权头传递）：
//   test-key           → 正常 SSE 流（"你好世界" 拆 3 个 chunk）
//   test-key-bad-json  → 中间混入 data: {broken 坏行，后续正常
//   test-key-401       → HTTP 401
//   test-key-500       → HTTP 500
//
// 错误时机: streamChat 是 async generator，非 2xx 的 fetch 错误在开始迭代时抛，
// 因此错误断言必须包在 for await 迭代里（try/catch）。

const assert = require('node:assert/strict');
const http = require('node:http');

/** 构造一条 SSE data 行（带增量 content 的 chat.completion.chunk）。 */
function sseChunk(content) {
  const payload = {
    id: 'test-id',
    object: 'chat.completion.chunk',
    choices: [{ index: 0, delta: { content }, finish_reason: null }],
  };
  return `data: ${JSON.stringify(payload)}\n\n`;
}

const SSE_DONE = 'data: [DONE]\n\n';

/**
 * 启动 mock SSE server（动态端口 127.0.0.1:0）。
 * 记录每次请求的 url/auth/body（requests 数组），按 Authorization Bearer 值分发场景。
 * @returns {Promise<{server: import('node:http').Server, port: number, requests: Array<{url: string, auth: string, body: object|null}>}>}
 */
function startMockServer() {
  /** @type {Array<{url: string, auth: string, body: object|null}>} */
  const requests = [];

  const server = http.createServer((req, res) => {
    let raw = '';
    req.on('data', (chunk) => {
      raw += chunk;
    });
    req.on('end', () => {
      let body = null;
      try {
        body = JSON.parse(raw);
      } catch {
        // 坏 body 不影响记录，测试断言时可见
      }
      const auth = req.headers.authorization || '';
      const bearer = auth.replace(/^Bearer /, '');
      requests.push({ url: req.url, auth, body });

      if (bearer === 'test-key-401') {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: { message: 'Unauthorized', type: 'authentication_error' } }));
        return;
      }
      if (bearer === 'test-key-500') {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: { message: 'Internal Server Error', type: 'server_error' } }));
        return;
      }

      res.writeHead(200, { 'Content-Type': 'text/event-stream' });
      if (bearer === 'test-key-bad-json') {
        // 坏行 data: {broken 夹在正常 chunk 之间，客户端应跳过并继续
        res.write(sseChunk('你'));
        res.write('data: {broken\n\n');
        res.write(sseChunk('好世界'));
        res.write(SSE_DONE);
        res.end();
        return;
      }
      // 正常流：回复 "你好世界" 拆 3 个 chunk
      res.write(sseChunk('你'));
      res.write(sseChunk('好世'));
      res.write(sseChunk('界'));
      res.write(SSE_DONE);
      res.end();
    });
  });

  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      resolve({ server, port, requests });
    });
  });
}

/** 完整迭代一个流式生成器，累积所有 content 增量。 */
async function collectStream(stream) {
  let content = '';
  for await (const chunk of stream) {
    if (chunk && typeof chunk.content === 'string') content += chunk.content;
  }
  return content;
}

/** 迭代流直到抛错，返回 Error；未抛错返回 null。 */
async function expectStreamError(stream) {
  try {
    for await (const _chunk of stream) {
      // 错误在迭代时抛出（fetch 在 generator 内），这里触发它
    }
    return null;
  } catch (err) {
    return err;
  }
}

async function main() {
  const { createOpenAiClient } = await import('../src/llm/openai-client.js');
  const { server, port, requests } = await startMockServer();

  let passed = 0;
  let failed = 0;
  /** @type {Array<{name: string, err: Error}>} */
  const failures = [];

  async function check(name, fn) {
    try {
      await fn();
      passed += 1;
      console.log(`PASS: ${name}`);
    } catch (err) {
      failed += 1;
      failures.push({ name, err });
      console.log(`FAIL: ${name} — ${err.message}`);
    }
  }

  try {
    const baseUrl = `http://127.0.0.1:${port}`;
    const messages = [{ role: 'user', content: 'hello' }];

    const client = createOpenAiClient({ baseUrl, apiKey: 'test-key', model: 'test-model' });
    const clientBadJson = createOpenAiClient({ baseUrl, apiKey: 'test-key-bad-json', model: 'test-model' });
    const client401 = createOpenAiClient({ baseUrl, apiKey: 'test-key-401', model: 'test-model' });
    const client500 = createOpenAiClient({ baseUrl, apiKey: 'test-key-500', model: 'test-model' });

    // 场景 1：正常流累积 content 与期望一致
    await check('正常流：streamChat 累积 content 等于 "你好世界"', async () => {
      const content = await collectStream(client.streamChat(messages));
      assert.equal(content, '你好世界', `got ${JSON.stringify(content)}`);
    });

    // 场景 2：请求体校验（thinking 显式 disabled + stream 开启）
    await check('请求体：thinking {type: disabled} + stream true + model/messages 正确', () => {
      const first = requests[0];
      assert.ok(first, 'no request recorded');
      assert.equal(first.url, '/chat/completions', `url=${first.url}`);
      assert.ok(first.body, 'body not JSON-parsable');
      assert.equal(first.body.stream, true, `stream=${first.body.stream}`);
      assert.deepEqual(first.body.thinking, { type: 'disabled' }, `thinking=${JSON.stringify(first.body.thinking)}`);
      assert.equal(first.body.model, 'test-model', `model=${first.body.model}`);
      assert.equal(first.body.messages.length, 1, `messages.length=${first.body.messages.length}`);
      assert.equal(first.body.messages[0].content, 'hello');
    });

    // 场景 3：非法 JSON 行跳过不崩溃，后续 content 正常累积
    await check('非法 JSON 行：跳过坏行不崩溃，后续 content 正常累积', async () => {
      const content = await collectStream(clientBadJson.streamChat(messages));
      assert.equal(content, '你好世界', `got ${JSON.stringify(content)}`);
    });

    // 场景 4：401 → 迭代时抛 Error 含 status 401
    await check('错误场景：HTTP 401 → 抛 Error 含 status 401', async () => {
      const err = await expectStreamError(client401.streamChat(messages));
      assert.ok(err, 'expected streamChat to throw on 401');
      assert.match(err.message, /401/, `message=${err.message}`);
    });

    // 场景 5：500 → 迭代时抛 Error 含 status 500
    await check('错误场景：HTTP 500 → 抛 Error 含 status 500', async () => {
      const err = await expectStreamError(client500.streamChat(messages));
      assert.ok(err, 'expected streamChat to throw on 500');
      assert.match(err.message, /500/, `message=${err.message}`);
    });

    // 场景 6：鉴权头传递（mock 按 Bearer 值分发场景，能区分即证明 header 正确携带）
    await check('鉴权头：Authorization 为 Bearer test-key（apiKey 注入）', () => {
      assert.equal(requests[0].auth, 'Bearer test-key', `auth=${requests[0].auth}`);
      assert.equal(requests[1].auth, 'Bearer test-key-bad-json', `auth=${requests[1].auth}`);
    });
  } finally {
    // 测试结束关闭 mock server（Node 18.2+ 先断 keep-alive 连接，避免 close 挂起）
    if (typeof server.closeAllConnections === 'function') server.closeAllConnections();
    server.close();
  }

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
  console.log('PASS: LLM 客户端流式链路跑通（mock SSE → createOpenAiClient → streamChat）');
}

main().catch((err) => {
  console.error(`[ERROR] ${err && err.stack ? err.stack : err}`);
  process.exit(1);
});
