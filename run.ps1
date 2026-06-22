#!/usr/bin/env pwsh
#
# run.ps1 — boot a 2-node ISOLATED CE test mesh and run the cross-node assertion driver (Windows).
#
# Cross-platform PowerShell (pwsh 7+) port of run.sh. It NEVER touches the live node on
# 127.0.0.1:8844: both test nodes run on unique high ports with --no-mdns (so they cannot
# cross-link the live node via LAN discovery) and on throwaway --data-dir / --ephemeral chains.
#
#   node A: api 18901, p2p 14901  (bootstrap seed)
#   node B: api 18902, p2p 14902  (--bootstrap <A multiaddr>)
#
# Both are started with: --no-mine --ephemeral --no-mdns.
#
# Rerunnable: ports are overridable; every node + temp dir is killed/removed on exit (even on
# failure / Ctrl-C) via try/finally.
#
# Usage:
#   ./run.ps1
#   $env:CE_BIN='C:\path\to\ce.exe'; ./run.ps1
#   $env:A_API='18911'; $env:B_API='18912'; ./run.ps1
#
# Exit code mirrors the driver: 0 = all non-blocked scenarios passed.

[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$Here = $PSScriptRoot

# On Windows the released binary is ce.exe; on unix it is ce. Honor CE_BIN if set.
# $IsWindows is an automatic var in pwsh 6+; on Windows PowerShell 5.1 it is absent, so probe safely.
$OnWindows = if (Test-Path 'variable:IsWindows') { $IsWindows } else { $true }
$ExeSuffix = if ($OnWindows) { '.exe' } else { '' }
$CeBin = if ($env:CE_BIN) {
    $env:CE_BIN
}
else {
    Join-Path (Split-Path -Parent $Here) "ce/target/release/ce$ExeSuffix"
}

$AApi = if ($env:A_API) { [int]$env:A_API } else { 18901 }
$AP2p = if ($env:A_P2P) { [int]$env:A_P2P } else { 14901 }
$BApi = if ($env:B_API) { [int]$env:B_API } else { 18902 }
$BP2p = if ($env:B_P2P) { [int]$env:B_P2P } else { 14902 }

$script:ADir = $null
$script:BDir = $null
$script:AProc = $null
$script:BProc = $null

function Write-Log([string]$msg) { Write-Host "[harness] $msg" }
function Stop-Harness([string]$msg) { Write-Host "[harness] ERROR: $msg"; throw $msg }

function Invoke-Cleanup {
    Write-Log "tearing down..."
    foreach ($proc in @($script:BProc, $script:AProc)) {
        if ($proc -and -not $proc.HasExited) {
            try { $proc.Kill($true) } catch { }
        }
    }
    Start-Sleep -Seconds 1
    foreach ($d in @($script:ADir, $script:BDir)) {
        if ($d -and (Test-Path $d)) {
            Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $d
        }
    }
    Write-Log "done"
}

function Wait-Health([int]$Port, [string]$Name, [System.Diagnostics.Process]$Proc, [string]$LogF) {
    $url = "http://127.0.0.1:$Port/health"
    for ($i = 0; $i -lt 60; $i++) {
        try {
            Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 2 | Out-Null
            return
        }
        catch { }
        if ($Proc.HasExited) {
            Write-Log "---- $Name died; log tail ----"
            if (Test-Path $LogF) { Get-Content $LogF -Tail 40 }
            Stop-Harness "$Name process exited before becoming healthy"
        }
        Start-Sleep -Milliseconds 500
    }
    Write-Log "---- $Name log tail ----"
    if (Test-Path $LogF) { Get-Content $LogF -Tail 40 }
    Stop-Harness "$Name never became healthy on :$Port"
}

function Read-Token([string]$Dir, [string]$Name) {
    $tf = Join-Path $Dir 'api.token'
    for ($i = 0; $i -lt 40; $i++) {
        if ((Test-Path $tf) -and (Get-Item $tf).Length -gt 0) {
            return ((Get-Content $tf -Raw) -replace '\s', '')
        }
        Start-Sleep -Milliseconds 250
    }
    Stop-Harness "$Name api.token never appeared in $Dir"
}

try {
    if (-not (Test-Path $CeBin)) {
        Stop-Harness "ce binary not found at $CeBin (set CE_BIN=...)"
    }

    # Guard: do not collide with the live node's default ports.
    foreach ($p in @($AApi, $BApi)) {
        if ($p -eq 8844) { Stop-Harness "refusing to use the live API port 8844" }
    }
    foreach ($p in @($AP2p, $BP2p)) {
        if ($p -eq 4001) { Stop-Harness "refusing to use the live P2P port 4001" }
    }

    # Build the driver (release) up front so a compile error fails fast and cheap.
    Write-Log "building integration driver..."
    Push-Location $Here
    try {
        cargo build --release --quiet
        if ($LASTEXITCODE -ne 0) { Stop-Harness "driver build failed" }
    }
    finally { Pop-Location }

    # cargo honors CARGO_TARGET_DIR (a shared target dir is configured in this workspace), so
    # resolve the actual binary location rather than assuming ./target.
    $targetDir = if ($env:CARGO_TARGET_DIR) { $env:CARGO_TARGET_DIR } else { Join-Path $Here 'target' }
    $Driver = Join-Path $targetDir "release/ce-integration$ExeSuffix"
    if (-not (Test-Path $Driver)) { Stop-Harness "driver binary missing at $Driver" }

    $script:ADir = New-Item -ItemType Directory -Force -Path (Join-Path ([System.IO.Path]::GetTempPath()) ([System.IO.Path]::GetRandomFileName())) | Select-Object -ExpandProperty FullName
    $script:BDir = New-Item -ItemType Directory -Force -Path (Join-Path ([System.IO.Path]::GetTempPath()) ([System.IO.Path]::GetRandomFileName())) | Select-Object -ExpandProperty FullName
    $ALog = Join-Path $script:ADir 'node.log'
    $BLog = Join-Path $script:BDir 'node.log'

    # ---- boot node A (the bootstrap seed) ----
    Write-Log "starting node A (api :$AApi, p2p :$AP2p)..."
    $script:AProc = Start-Process -FilePath $CeBin -PassThru -NoNewWindow `
        -RedirectStandardOutput $ALog -RedirectStandardError "$ALog.err" `
        -ArgumentList @('--data-dir', $script:ADir, 'start', '--no-mine', '--ephemeral', '--no-mdns', '--api-port', "$AApi", '--port', "$AP2p")
    Wait-Health $AApi 'A' $script:AProc $ALog
    $ATok = Read-Token $script:ADir 'A'

    # Derive A's connectable multiaddr from /bootstrap; splice in A's loopback listen address.
    $ABootRaw = (Invoke-WebRequest -Uri "http://127.0.0.1:$AApi/bootstrap" -UseBasicParsing).Content
    $m = [regex]::Match($ABootRaw, '/p2p/([A-Za-z0-9]+)')
    if (-not $m.Success) { Stop-Harness "could not parse A peer id from /bootstrap: $ABootRaw" }
    $APeerId = $m.Groups[1].Value
    $AMultiaddr = "/ip4/127.0.0.1/tcp/$AP2p/p2p/$APeerId"
    Write-Log "node A multiaddr: $AMultiaddr"

    # ---- boot node B, bootstrapped to A ----
    Write-Log "starting node B (api :$BApi, p2p :$BP2p) bootstrapped to A..."
    $script:BProc = Start-Process -FilePath $CeBin -PassThru -NoNewWindow `
        -RedirectStandardOutput $BLog -RedirectStandardError "$BLog.err" `
        -ArgumentList @('--data-dir', $script:BDir, 'start', '--no-mine', '--ephemeral', '--no-mdns', '--api-port', "$BApi", '--port', "$BP2p", '--bootstrap', $AMultiaddr)
    Wait-Health $BApi 'B' $script:BProc $BLog
    $BTok = Read-Token $script:BDir 'B'

    # Mint a tunnel capability: B (resource owner) self-issues a `tunnel` capability to A's NodeId.
    Write-Log "minting a tunnel capability: B -> A..."
    $AStatus = (Invoke-WebRequest -Uri "http://127.0.0.1:$AApi/status" -UseBasicParsing).Content
    $sm = [regex]::Match($AStatus, '"node_id":"([0-9a-f]+)"')
    if (-not $sm.Success) { Stop-Harness "could not read A node_id from /status" }
    $ANodeId = $sm.Groups[1].Value
    $TunnelCaps = ''
    try {
        $grantOut = & $CeBin --data-dir $script:BDir grant $ANodeId --can tunnel 2>$null
        $TunnelCaps = (($grantOut | Out-String) -replace '\s', '')
    }
    catch { }
    if (-not $TunnelCaps) {
        Write-Log "WARNING: tunnel capability mint produced no token; tunnel scenario will run uncapped (likely BLOCKED)"
    }

    Write-Log "both nodes healthy; handing off to the assertion driver."

    # ---- run the driver ----
    $env:CE_IT_A_BASE = "http://127.0.0.1:$AApi"
    $env:CE_IT_A_TOKEN = $ATok
    $env:CE_IT_B_BASE = "http://127.0.0.1:$BApi"
    $env:CE_IT_B_TOKEN = $BTok
    $env:CE_IT_TUNNEL_CAPS = $TunnelCaps
    if (-not $env:RUST_LOG) { $env:RUST_LOG = 'info' }

    & $Driver
    $RC = $LASTEXITCODE

    if ($RC -ne 0) {
        Write-Log "driver exited non-zero ($RC); node A log tail:"
        if (Test-Path $ALog) { Get-Content $ALog -Tail 25 }
        Write-Log "node B log tail:"
        if (Test-Path $BLog) { Get-Content $BLog -Tail 25 }
    }

    exit $RC
}
finally {
    Invoke-Cleanup
}
