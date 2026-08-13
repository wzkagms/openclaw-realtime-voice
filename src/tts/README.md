# src/tts — edge-tts-universal + mpg123 解码（Phase 2）

TTS 输出链路：

- edge-tts-universal@1.4.0 流式（晓晓 24k mp3，实测 1.67s 首包）
- mpg123-decoder@1.0.3 解码 → PCM → onAudio 分块推送（Control UI playhead 契约）
- 指数退避重试 → sherpa 本地 TTS fallback → SAPI 终极兜底

Phase 2 填充。当前为空骨架。
