# src/bridge — 状态机 + 音频管道（Phase 1）

realtime voice provider bridge 核心逻辑：

- **状态机**：IDLE / LISTENING / RECOGNIZING / THINKING / SPEAKING
- **音频管道**：resampler 24k→16k → sherpa 流式 STT → endpoint 出句 → VAD barge-in 打断
- 契约：`api.registerRealtimeVoiceProvider` 的 `createBridge(req)` 返回对象在 index.js 中组装，内部实现落位本目录

Phase 1 填充。当前为空骨架。
