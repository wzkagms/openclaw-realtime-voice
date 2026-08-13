# download-models.ps1 - 一键下载并解压 sherpa STT 流式模型（约 530MB）
#
# 用法:
#   powershell -File scripts/download-models.ps1            # 正常下载 -> 解压 -> 校验 -> 清理
#   powershell -File scripts/download-models.ps1 -DryRun    # 只 HEAD 验证 URL 可达 + 打印预期文件名，不下载
#
# 说明:
#   - 模型来源: sherpa-onnx 官方 GitHub Release（k2-fsa/sherpa-onnx）
#   - 解压后目录: models/sherpa-onnx-streaming-zipformer-bilingual-zh-en-2023-02-20/
#     （与 src/bridge/bridge-runtime.js DEFAULT_MODEL_PATH / src/stt/sherpa-stt.js 文件常量一致）
#   - 依赖系统自带 curl.exe / tar.exe（Win10 1803+），无需安装 7z/bsdtar

param(
    [switch]$DryRun
)

$ErrorActionPreference = 'Stop'

# ---------- 常量 ----------
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$ModelsDir   = Join-Path $ProjectRoot 'models'
$ModelDirName = 'sherpa-onnx-streaming-zipformer-bilingual-zh-en-2023-02-20'
$ModelDir    = Join-Path $ModelsDir $ModelDirName
$ModelUrl    = 'https://github.com/k2-fsa/sherpa-onnx/releases/download/asr-models/sherpa-onnx-streaming-zipformer-bilingual-zh-en-2023-02-20.tar.bz2'
$ArchiveName = 'sherpa-onnx-streaming-zipformer-bilingual-zh-en-2023-02-20.tar.bz2'
$ArchivePath = Join-Path $ModelsDir $ArchiveName

# 必需文件（与 src/stt/sherpa-stt.js 中 ENCODER_FILE/DECODER_FILE/JOINER_FILE/TOKENS_FILE 一致）
$RequiredFiles = @(
    'encoder-epoch-99-avg-1.int8.onnx',
    'decoder-epoch-99-avg-1.int8.onnx',
    'joiner-epoch-99-avg-1.int8.onnx',
    'tokens.txt'
)

# ---------- 工具函数 ----------
function Write-Step([string]$msg) { Write-Host "==> $msg" }

function Write-Err([string]$msg) { Write-Host "[错误] $msg" -ForegroundColor Red }

# 校验目标目录是否已包含全部必需文件；返回缺失文件列表
function Get-MissingFiles {
    $missing = @($RequiredFiles | Where-Object { -not (Test-Path (Join-Path $ModelDir $_)) })
    return ,$missing
}

# HEAD 请求验证 URL 可达；返回最终 HTTP 状态码（跟随重定向）
function Test-UrlReachable {
    $curl = Get-Command curl.exe -ErrorAction SilentlyContinue
    if ($curl) {
        # curl.exe -I -L 跟随 GitHub 302 重定向，取最终状态码
        $code = (& $curl.Source -sIL -o NUL -w '%{http_code}' $ModelUrl).Trim()
        return $code
    }
    # 兜底：Invoke-WebRequest HEAD（自动跟随重定向）
    try {
        $resp = Invoke-WebRequest -Uri $ModelUrl -Method Head -MaximumRedirection 5 -UseBasicParsing
        return [string][int]$resp.StatusCode
    } catch {
        return 'ERR'
    }
}

# ---------- 主流程 ----------
try {
    # ---- DryRun 模式：只验证 URL，不下载（零副作用，不创建目录）----
    if ($DryRun) {
        Write-Step 'DryRun 模式：仅 HEAD 验证 URL 可达（不下载任何文件）'
        $code = Test-UrlReachable
        if ($code -eq '200') {
            Write-Host "URL 可达: $ModelUrl  (HTTP $code)" -ForegroundColor Green
        } else {
            Write-Err "URL 不可达（HTTP $code）: $ModelUrl"
            exit 1
        }
        Write-Host "预期解压目录: $ModelDirName"
        Write-Host '预期必需文件（与 src/stt/sherpa-stt.js 一致）:'
        foreach ($f in $RequiredFiles) {
            Write-Host "  - $f"
        }
        Write-Host 'DryRun 完成，未下载任何文件。'
        exit 0
    }

    # ---- 幂等检查：目标目录已存在且 4 个必需文件齐全 -> 跳过 ----
    $missing = Get-MissingFiles
    if ((Test-Path $ModelDir) -and $missing.Count -eq 0) {
        Write-Host "模型已存在: $ModelDir （4 个必需文件齐全），跳过下载。" -ForegroundColor Green
        exit 0
    }

    # ---- 下载（约 530MB）----
    if (-not (Test-Path $ModelsDir)) {
        New-Item -ItemType Directory -Path $ModelsDir -Force | Out-Null
    }
    Write-Step "开始下载模型（约 530MB）..."
    Write-Host "URL: $ModelUrl"
    $curl = Get-Command curl.exe -ErrorAction SilentlyContinue
    if ($curl) {
        # -f 失败即返回非零；-S 显示错误；-L 跟随重定向；--retry 3 网络抖动重试
        & $curl.Source -fSL --retry 3 -o $ArchivePath $ModelUrl
        if ($LASTEXITCODE -ne 0) { throw "curl 下载失败（exit code $LASTEXITCODE）" }
    } else {
        Invoke-WebRequest -Uri $ModelUrl -OutFile $ArchivePath -UseBasicParsing
    }
    Write-Host "下载完成: $ArchivePath"

    # ---- 解压（系统自带 tar，Win10 1803+ 支持 -xjf）----
    Write-Step '解压...'
    tar -xjf $ArchivePath -C $ModelsDir
    if ($LASTEXITCODE -ne 0) { throw "tar 解压失败（exit code $LASTEXITCODE）" }

    # ---- 校验必需文件 ----
    Write-Step '校验必需文件...'
    $missing = Get-MissingFiles
    if ($missing.Count -gt 0) {
        Write-Err "模型文件缺失（$($missing.Count) 个）:"
        foreach ($m in $missing) {
            Write-Host "  - $m" -ForegroundColor Red
        }
        throw '必需文件缺失，请检查解压结果或重新下载'
    }

    # ---- 清理压缩包 ----
    Write-Step '清理压缩包...'
    Remove-Item $ArchivePath -Force
    Write-Host "模型就绪: $ModelDir" -ForegroundColor Green
    exit 0
}
catch {
    Write-Err $_.Exception.Message
    # 清理失败残留的压缩包（若存在）
    if (Test-Path $ArchivePath) {
        Remove-Item $ArchivePath -Force -ErrorAction SilentlyContinue
    }
    exit 1
}
