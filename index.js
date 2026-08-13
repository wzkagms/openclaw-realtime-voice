// tts-plugin realtime voice provider entry
// Phase 1: entry is a thin parameter-parsing + delegation layer.
// All bridge logic lives in src/bridge/bridge-runtime.js (sendAudio/handleBargeIn/close).
import { definePluginEntry } from "openclaw/plugin-sdk/plugin-entry";
import { createBridgeRuntime } from "./src/bridge/bridge-runtime.js";

export default definePluginEntry({
  id: "tts-plugin",
  name: "TTS Plugin Realtime Voice",
  description:
    "Realtime voice provider: browser mic -> sherpa STT -> LLM -> edge-tts -> audio playback",
  register(api) {
    api.registerRealtimeVoiceProvider({
      id: "tts-plugin",
      label: "TTS Plugin Voice",
      capabilities: {
        // Phase 0: gateway-relay transport per dev-plan (gateway-relay transport).
        // TODO Phase 3: confirm input sample rate (getUserMedia default 48k?) and align with bridge inputSampleRate.
        transports: ["gateway-relay"],
        inputAudioFormats: [
          { encoding: "pcm16", sampleRateHz: 24000, channels: 1 },
        ],
        outputAudioFormats: [
          { encoding: "pcm16", sampleRateHz: 24000, channels: 1 },
        ],
        supportsBargeIn: true,
        handlesInputAudioBargeIn: true,
        supportsToolCalls: true,
      },
      isConfigured: ({ providerConfig }) =>
        Boolean(providerConfig?.apiKey),
      createBridge: (req) => {
        // 入口薄层：只做参数解析 + 回调透传，业务逻辑全部在 src/bridge/bridge-runtime.js。
        // inputSampleRate：以 req 实际音频格式为准（SDK 硬约束 pcm16/24000），
        // 无则 24000 兜底（Phase 3 采样率对齐：gateway 已把 48k 重采样到声明的 24k）。
        // 回调可选性（undefined 安全）由 bridge-runtime 内部 safeCall 处理，薄层不额外 try/catch。
        const inputSampleRate = req?.audioFormat?.sampleRateHz ?? 24000;
        // Phase 4 agent-consult 适配：透传 SDK 新契约参数
        // （tools=openclaw_agent_consult/control 工具描述、autoRespondToAudio=是否自管回复、
        //  instructions=agent 会话提示词）。bridge 内部据此切换主路径/兜底。
        return createBridgeRuntime({
          providerConfig: req?.providerConfig,
          inputSampleRate,
          tools: req?.tools,
          autoRespondToAudio: req?.autoRespondToAudio,
          instructions: req?.instructions,
          onAudio: req?.onAudio,
          onTranscript: req?.onTranscript,
          onMark: req?.onMark,
          onClearAudio: req?.onClearAudio,
          onToolCall: req?.onToolCall,
          onEvent: req?.onEvent,
          onReady: req?.onReady,
          onError: req?.onError,
          onClose: req?.onClose,
        });
      },
    });
  },
});
