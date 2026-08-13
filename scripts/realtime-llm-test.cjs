// scripts/realtime-llm-test.cjs
// Phase 3 真实端点 LLM 测试（澜影裁决：opencode-go 端点 + env key）：
//   从 ~/.openclaw/openclaw.json 的 talk.realtime.providers.tts-plugin 读取三配置
//   （baseUrl/apiKey/model，apiKey 支持 SecretInput {source:env,id:...} 解析）
//   → createOpenAiClient 真实调用一次 streamChat（文本 → 回复文本）
//   验证：三配置真实生效 + /v1 前缀兼容性（baseUrl 带 /v1 → buildChatUrl 拼接）
// Phase 4 扩展：CLI 覆盖（对照官方端点用，不落盘、不改 openclaw.json）：
//   node scripts/realtime-llm-test.cjs [--endpoint <baseUrl>] [--model <model>] [--api-key <key>]
//   用法: node scripts/realtime-llm-test.cjs
//   对照: node scripts/realtime-llm-test.cjs --endpoint https://api.deepseek.com/v1 --model deepseek-chat --api-key $DEEPSEEK_API_KEY
// 退出码: 0 = 调用成功(2xx + 非空回复), 1 = 失败（响应原文脱敏后输出，失败也算有结论）
//
// 说明:
//   - 真实网络 + 真实 API key（资源决策已由澜影批准）；key 不落日志、输出脱敏。
//   - 与 e2e-test.cjs（mock LLM 稳定基线）互补：本脚本验证真实端点三配置链路。

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

/** 读取 OpenClaw 全局配置（~/.openclaw/openclaw.json）。 */
function loadOpenClawConfig() {
  const p = path.join(os.homedir(), '.openclaw', 'openclaw.json');
  return JSON.parse(fs.readFileSync(p, 'utf8'));
}

/**
 * 解析 SecretInput：{source:"env", id:"OPENCODE_API_KEY"} → 从 openclaw.json env 段或
 * 进程环境变量读取实际值；普通字符串原样返回。
 */
function resolveSecretInput(value, cfg) {
  if (value && typeof value === 'object' && value.source === 'env') {
    return cfg.env?.[value.id] ?? process.env[value.id] ?? '';
  }
  return value ?? '';
}

/** 脱敏：只保留前 4 后 4，中间打码。 */
function maskKey(key) {
  if (typeof key !== 'string' || key.length <= 8) return '***';
  return `${key.slice(0, 4)}...${key.slice(-4)}`;
}

/** 解析 CLI 覆盖参数：--endpoint <url> / --model <id> / --api-key <key>（均可选）。 */
function parseCliArgs(argv) {
  const args = { endpoint: null, model: null, apiKey: null };
  for (let i = 2; i < argv.length; i++) {
    const flag = argv[i];
    const value = argv[i + 1];
    if (flag === '--endpoint' && value && !value.startsWith('--')) {
      args.endpoint = value;
      i += 1;
    } else if (flag === '--model' && value && !value.startsWith('--')) {
      args.model = value;
      i += 1;
    } else if (flag === '--api-key' && value && !value.startsWith('--')) {
      args.apiKey = value;
      i += 1;
    } else if (flag === '--help' || flag === '-h') {
      console.log('用法: node scripts/realtime-llm-test.cjs [--endpoint <baseUrl>] [--model <model>] [--api-key <key>]');
      console.log('  无参数: 读 ~/.openclaw/openclaw.json 的 talk.realtime.providers.tts-plugin 三配置');
      console.log('  对照端点: 传 --endpoint/--model/--api-key 覆盖（不落盘，不改 openclaw.json）');
      process.exit(0);
    }
  }
  return args;
}

async function main() {
  const cfg = loadOpenClawConfig();
  const provider = cfg.talk?.realtime?.providers?.['tts-plugin'];
  const cli = parseCliArgs(process.argv);
  if (!provider && !cli.endpoint) {
    console.error('[ERROR] talk.realtime.providers.tts-plugin not found in ~/.openclaw/openclaw.json (and no --endpoint override)');
    process.exitCode = 1;
    return;
  }
  const baseUrl = cli.endpoint ?? provider.baseUrl;
  const apiKey = cli.apiKey ?? resolveSecretInput(provider.apiKey, cfg);
  const model = cli.model ?? provider.model;
  if (!baseUrl || !apiKey || !model) {
    console.error(`[ERROR] provider config incomplete: baseUrl=${Boolean(baseUrl)}, apiKey=${Boolean(apiKey)}, model=${Boolean(model)}`);
    process.exitCode = 1;
    return;
  }

  console.log(`[realtime-llm] baseUrl: ${baseUrl}`);
  console.log(`[realtime-llm] apiKey: ${maskKey(apiKey)} (len=${apiKey.length})`);
  console.log(`[realtime-llm] model: ${model}`);
  console.log(`[realtime-llm] endpoint: ${baseUrl.replace(/\/+$/, '')}/chat/completions (with /v1 prefix as-is)`);
  console.log(`[realtime-llm] source: ${cli.endpoint ? 'CLI override (对照验证, 未落盘)' : 'openclaw.json talk.realtime'}`);

  const { createOpenAiClient } = await import('../src/llm/openai-client.js');
  const client = createOpenAiClient({ baseUrl, apiKey, model });

  const start = Date.now();
  let reply = '';
  let chunkCount = 0;
  try {
    for await (const chunk of client.streamChat([{ role: 'user', content: '你好，请用一句话回复确认语音链路可用。' }])) {
      if (chunk.content) {
        reply += chunk.content;
        chunkCount += 1;
      }
    }
  } catch (error) {
    const elapsedMs = Date.now() - start;
    console.error(`\n[FAIL] realtime LLM call failed (${elapsedMs}ms): ${error.message}`);
    console.error('      非 2xx 也算结论：三配置链路已真实请求，端点拒绝。');
    process.exitCode = 1;
    return;
  }

  const elapsedMs = Date.now() - start;
  const ok = reply.trim().length > 0;
  console.log(`\n[realtime-llm] HTTP 2xx + stream OK in ${elapsedMs}ms (${chunkCount} chunks)`);
  console.log(`[realtime-llm] reply: "${reply.trim()}"`);
  console.log(`[realtime-llm] /v1 前缀兼容性: ${baseUrl.includes('/v1') ? '带 /v1 且调用成功 ✅' : '不带 /v1（同样兼容）✅'}`);

  if (!ok) {
    console.error('[FAIL] reply empty');
    process.exitCode = 1;
    return;
  }
  console.log('PASS: realtime LLM 真实端点三配置（baseUrl/apiKey/model）生效 + /v1 前缀兼容');
}

main().catch((err) => {
  console.error(`[ERROR] ${err && err.stack ? err.stack : err}`);
  process.exit(1);
});
