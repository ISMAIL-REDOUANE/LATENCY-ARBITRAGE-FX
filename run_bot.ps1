#run_bot.ps1
# Windows one-liner to deploy + run the lead_lag bot inside WSL (Ubuntu-24.04).
# Pulls the repo, Release-builds lead_lag with CMake/Ninja, then starts it in
# DRY_RUN against the IC Markets cTrader FIX gateway.
#
#   .\run_bot.ps1                 # BINANCE feed, build then run
#   .\run_bot.ps1 -FastFeed BYBIT # Bybit v5 orderbook.1 feed instead
#   .\run_bot.ps1 -SkipBuild      # reuse the existing binary
#   .\run_bot.ps1 -LiveTrades     # FIX_ENABLED=1 (DRY_RUN=0)
#
# Settings come from the local PowerShell environment (FIX_PASSWORD,
# FIX_ACCOUNT_ID, FIX_SENDER_COMP_ID, FIX_TARGET_COMP_ID, FIX_HOST, FIX_PORT)
# or their matching -Fix* parameters. Values cross into WSL base64-encoded via
# WSLENV (space/metachar-safe), and a generated bash script is driven through
# WSL's access to /mnt/c. The current dir is used for the temp script.

param(
    [ValidateSet("BINANCE", "BYBIT")]
    [string]$FastFeed,
    [string]$FixPassword   = $env:FIX_PASSWORD,
    [string]$FixAccountId  = $env:FIX_ACCOUNT_ID,
    [string]$FixSenderCompId = $env:FIX_SENDER_COMP_ID,
    [string]$FixTargetCompId  = $env:FIX_TARGET_COMP_ID,
    [string]$FixHost          = $env:FIX_HOST,
    [string]$FixPort          = $env:FIX_PORT,
    [string]$RepoDir          = "/opt/LATENCY-ARBITRAGE-FX/latency_arb",
    [switch]$SkipBuild,
    [switch]$LiveTrades       = $false
)

$ErrorActionPreference = "Stop"

if (-not $FastFeed) { $FastFeed = if ($env:FAST_FEED) { $env:FAST_FEED } else { "BINANCE" } }
if (-not $FixAccountId) { throw "FIX_ACCOUNT_ID is required (account number, e.g. 10092442)." }
if (-not $FixPassword)  { throw "FIX_PASSWORD is required (IC Markets cTrader FIX password)." }

if (-not $FixSenderCompId) { $FixSenderCompId = "demo.icmarkets.$FixAccountId" }
if (-not $FixTargetCompId) { $FixTargetCompId  = "cServer" }
if (-not $FixHost)         { $FixHost          = "demo-uk-eqx-01.p.c-trader.com" }
if (-not $FixPort)         { $FixPort          = "5211" }

function Set-B64 { param([string]$Name, [string]$Value) Set-Item -Path "env:$Name" -Value ([Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($Value))) }

$dry   = if ($LiveTrades) { "0" } else { "1" }
$fixen = if ($LiveTrades) { "1" } else { "0" }

(@{
    B64_FAST_FEED          = $FastFeed
    B64_FIX_PASSWORD       = $FixPassword
    B64_FIX_ACCOUNT_ID     = $FixAccountId
    B64_FIX_SENDER_COMP_ID = $FixSenderCompId
    B64_FIX_TARGET_COMP_ID = $FixTargetCompId
    B64_FIX_HOST           = $FixHost
    B64_FIX_PORT           = $FixPort
    B64_DRY_RUN            = $dry
    B64_FIX_ENABLED        = $fixen
}).GetEnumerator() | ForEach-Object { Set-B64 $_.Key $_.Value }

$env:WSLENV = "B64_FAST_FEED/u:B64_FIX_PASSWORD/u:B64_FIX_ACCOUNT_ID/u:B64_FIX_SENDER_COMP_ID/u:B64_FIX_TARGET_COMP_ID/u:B64_FIX_HOST/u:B64_FIX_PORT/u:B64_DRY_RUN/u:B64_FIX_ENABLED/u"

$tmp = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".run_bot_wsl.sh"))

# Single-quoted here-string: no interpolation, so bash's $(...) survives.
$body = @'
export FAST_FEED=$(printf '%s' "$B64_FAST_FEED" | base64 -d)
export FIX_PASSWORD=$(printf '%s' "$B64_FIX_PASSWORD" | base64 -d)
export FIX_ACCOUNT_ID=$(printf '%s' "$B64_FIX_ACCOUNT_ID" | base64 -d)
export FIX_SENDER_COMP_ID=$(printf '%s' "$B64_FIX_SENDER_COMP_ID" | base64 -d)
export FIX_TARGET_COMP_ID=$(printf '%s' "$B64_FIX_TARGET_COMP_ID" | base64 -d)
export FIX_HOST=$(printf '%s' "$B64_FIX_HOST" | base64 -d)
export FIX_PORT=$(printf '%s' "$B64_FIX_PORT" | base64 -d)
export DRY_RUN=$(printf '%s' "$B64_DRY_RUN" | base64 -d)
export FIX_ENABLED=$(printf '%s' "$B64_FIX_ENABLED" | base64 -d)
export FIX_HEARTBEAT_S=30

cd __REPO_DIR__
git pull --ff-only
'@
$body += "`n"

if (-not $SkipBuild) {
    $body += @'
sudo rm -rf build
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
sudo cmake --build build --target lead_lag
'@
    $body += "`n"
}

$body += @'
exec ./build/lead_lag
'@

$script = $body.Replace("__REPO_DIR__", $RepoDir)
[System.IO.File]::WriteAllText($tmp, $script.Replace("`r", ""), [System.Text.UTF8Encoding]::new($false))

$drive = [System.IO.Path]::GetPathRoot($tmp).TrimEnd(':', '\')
$wslPath = ("/mnt/" + $drive.ToLower() + "/") + ($tmp.Substring(3)).Replace('\', '/')
Write-Host "Deploying + starting bot inside WSL (Ctrl+C stops the bot)."
Write-Host "FAST_FEED=$FastFeed  DRY_RUN=$dry  FIX_HOST=$FixHost"
wsl.exe bash $wslPath
$code = $LASTEXITCODE
Remove-Item -Path $tmp -ErrorAction SilentlyContinue
exit $code