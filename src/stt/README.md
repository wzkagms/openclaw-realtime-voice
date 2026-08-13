# src/stt — sherpa-onnx 封装（Phase 1）

sherpa-onnx-node 流式识别封装：

- OnlineRecognizer 初始化（streaming zipformer bilingual int8，models/ 已下载）
- 16k PCM 入 → 文本出（endpoint 出句）
- 参考脚本：`scripts/streaming-test.cjs`（验证过 RTF 0.056）

Phase 1 填充。当前为空骨架。
