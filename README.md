# openclaw-realtime-voice

OpenClaw 实时语音循环插件：浏览器麦克风 → sherpa STT 流式识别 → LLM 意图分类/清洗 → edge-tts TTS 合成 → 播放。零成本 ChatGPT 式语音循环。

包名 `openclaw-realtime-voice`；插件注册 id 为 `tts-plugin`（内部标识）。

## 功能特性

| # | 功能 | 说明 |
|---|---|---|
| 1 | STT 流式识别 | sherpa-onnx streaming（zipformer bilingual int8，16k/80 特征），能量 VAD + endpoint 检测出句 |
| 2 | edge-tts TTS 合成 | 流式 mp3 → mpg123 解码 → PCM16 LE 20ms 帧（960B @24k）→ 推送播放 |
| 3 | 等待语预合成 | 固定文案（在的/稍等/重说）预合成缓存，播放命中零合成延迟 |
| 4 | ack 两层 | 连接就绪「在的」+ 工具调用「稍等，我查一下」等待语 |
| 5 | 工具调用 consult | 上报 transcript → gateway 强制 consult → 回流 sendUserMessage → LLM 转口语 → TTS |
| 6 | 打断 barge-in | 递增 flowToken 取消在途 LLM/TTS + 清音频缓冲 + 回 LISTENING 继续听 |
| 7 | 回声抑制 | 播放锁 + 播放后 300ms 尾部静默窗 + barge-in 短抑制窗 |
| 8 | 预加载 | sherpa OnlineRecognizer 模块级单例（int8 ~190MB 常驻），会话间复用，消除每次 ~10s 模型加载 |
| 9 | 兜底 60s | consult 无回流 60s 后自管 LLM 回复；防双回复 |
| 10 | 错误风暴防护 | decode 前 free 校验 + 预合成 isCancelled 检查，防死循环 |

## 架构

bridge 纯函数状态机（`src/bridge/state-machine.js`）：

```
IDLE → LISTENING → RECOGNIZING → THINKING → SPEAKING → IDLE
                ↑        ↑           ↑          ↑
                └──────── BARGE_IN 任意态回 LISTENING（打断后继续听）┘
TEXT_INPUT：IDLE → THINKING（文本消息入口）
RESET/CLOSE：任意态 → IDLE（幂等）
```

消费端全链路：浏览器麦克风 → gateway-relay → audio-pipeline（PCM16→Float32→重采样 24→16k → 能量 VAD → sherpa feed）→ endpoint 出句 → LLM 分类（needTool/cleanedText）→ gateway 强制 consult 回流 → edge-tts → mpg123 解码 → 20ms PCM16 帧 → gateway → 浏览器 playhead 队列播放。

## 安装

```bash
npm install
```

依赖：`sherpa-onnx-node`（STT 识别）、`mpg123-decoder`（mp3 解码）、`edge-tts-universal`（TTS 合成）。peer 依赖 OpenClaw `>=2026.3.24-beta.2`。

## 配置

在 OpenClaw 的 `openclaw.json` 中注册插件并配置 provider：

```json
{
  "realtimeVoice": {
    "provider": "openclaw-realtime-voice",
    "providerConfig": {
      "baseUrl": "https://api.opencode.ai/v1",
      "apiKey": "process.env.DEEPSEEK_API_KEY",
      "model": "deepseek-v4-flash"
    }
  }
}
```

`apiKey` 支持 env 引用（如 `process.env.DEEPSEEK_API_KEY`），也支持 OpenAI 兼容任意端点。

## 模型下载

STT 需要 sherpa streaming 模型（约 530MB）。`models/` 目录已 gitignore，请自行下载到 `models/` 下，模型目录须包含以下文件（布局见 `src/stt/sherpa-stt.js`）：

```
models/
├── encoder-epoch-99-avg-1.int8.onnx
├── decoder-epoch-99-avg-1.int8.onnx
├── joiner-epoch-99-avg-1.int8.onnx
└── tokens.txt
```

启动时校验文件存在，缺失会报错。模型变更后需重启 gateway（识别器单例跨会话常驻）。

## 费用说明

- **LLM**：默认使用 opencode-go 付费 API（或自备任意 OpenAI 兼容端点，通过 `providerConfig` 配置）
- **TTS**：edge-tts 免费服务（微软 Edge 在线语音合成）

## 测试

181 通过 / 0 失败（18 套件）。全量回归：

```bash
for f in scripts/*test.cjs; do node $f; done
```

已知：`streaming-test` 存在既有崩溃（`stream.free is not a function`，sherpa-onnx-node 1.13.5 无 free 方法），不影响运行时，正式实现用 `recognizer.reset(stream)` 复用 stream。

## Credits

本项目由三位协作者共同完成：

- **澜影** — 产品设计与最终决策者
- **墨璃** — OpenClaw AI 助手：需求拆解、诊断、验收
- **云璃** — opencode AI 助手：实现、测试、文档

## License

MIT
