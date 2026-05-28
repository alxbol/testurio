<#
.SYNOPSIS
  Start kubectl port-forward to Kafka broker pod (test-stable) for local
  Step 7 of mt5-market-symbols-v2 smoke test.

.DESCRIPTION
  Run in a SEPARATE PowerShell window before the test. Keeps the tunnel
  alive while the test runs; Ctrl+C to stop. Auto-restarts on transient
  drop (loop with backoff).

  - Tunnel: pod/kafka-controller-0:9092 -> localhost:9092
  - Pre-flight: ensures the hosts file maps the advertised broker hostname
    to 127.0.0.1 (interactive elevation if missing).

.PARAMETER LocalPort
  Local TCP port to bind. Default 9092. Override if 9092 is busy:
    .\kafka-port-forward.ps1 -LocalPort 9094
  Then set KAFKA_BROKERS=localhost:9094 before running the test.

.PARAMETER Pod
  Broker pod name. Default kafka-controller-0.

.PARAMETER Kubeconfig
  Path to kubeconfig. Default C:/Users/abolshakov/Documents/k8s/kubeconfig.

.PARAMETER Kubectl
  Path to kubectl binary. Default C:/Users/abolshakov/bin/kubectl.exe.
#>

[CmdletBinding()]
param(
    [int]$LocalPort = 9092,
    [string]$Pod = "kafka-controller-0",
    [string]$Namespace = "test-stable",
    [string]$Kubeconfig = "C:/Users/abolshakov/Documents/k8s/kubeconfig",
    [string]$Kubectl = "C:/Users/abolshakov/bin/kubectl.exe"
)

$ErrorActionPreference = "Stop"

$AdvertisedHost = "$Pod.kafka-controller-headless.$Namespace.svc.cluster.local"
$HostsPath = "$env:windir\System32\drivers\etc\hosts"
$HostsLine = "127.0.0.1 $AdvertisedHost"

function Test-HostsEntry {
    if (-not (Test-Path $HostsPath)) { return $false }
    $content = Get-Content -Raw $HostsPath -ErrorAction SilentlyContinue
    return $content -match [regex]::Escape($AdvertisedHost)
}

function Add-HostsEntry {
    Write-Host "Adding hosts entry (requires admin)..." -ForegroundColor Yellow
    $cmd = "Add-Content -Path '$HostsPath' -Value '`n$HostsLine' -Encoding ASCII"
    try {
        Start-Process powershell -ArgumentList "-NoProfile", "-Command", $cmd -Verb RunAs -Wait
    } catch {
        Write-Host "Failed to elevate. Add this line manually to $HostsPath:" -ForegroundColor Red
        Write-Host "  $HostsLine" -ForegroundColor Cyan
        exit 1
    }
    if (-not (Test-HostsEntry)) {
        Write-Host "Hosts entry still missing after elevation. Aborting." -ForegroundColor Red
        exit 1
    }
    Write-Host "Hosts entry added." -ForegroundColor Green
}

# --- Pre-flight ---
if (-not (Test-Path $Kubectl)) {
    Write-Host "kubectl not found at $Kubectl" -ForegroundColor Red
    exit 1
}
if (-not (Test-Path $Kubeconfig)) {
    Write-Host "Kubeconfig not found at $Kubeconfig" -ForegroundColor Red
    exit 1
}
if (-not (Test-HostsEntry)) {
    Write-Host "Hosts entry missing: $HostsLine" -ForegroundColor Yellow
    Add-HostsEntry
} else {
    Write-Host "Hosts entry OK: $HostsLine" -ForegroundColor Green
}

# Ping check
$ping = Test-Connection -ComputerName $AdvertisedHost -Count 1 -Quiet -ErrorAction SilentlyContinue
if (-not $ping) {
    Write-Host "Hosts mapping unreachable via ICMP (firewall?). Continuing — kafkajs uses TCP." -ForegroundColor Yellow
}

Write-Host ""
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host " Kafka port-forward — keep this window open" -ForegroundColor Cyan
Write-Host " pod/$Pod:9092 -> localhost:$LocalPort" -ForegroundColor Cyan
Write-Host " For the test, run in another shell:" -ForegroundColor Cyan
if ($LocalPort -ne 9092) {
    Write-Host "   `$env:KAFKA_BROKERS='localhost:$LocalPort'" -ForegroundColor Gray
}
Write-Host "   npx vitest run tests/smoke/mt5-market-symbols-v2.integration.test.ts" -ForegroundColor Gray
Write-Host " Ctrl+C to stop." -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host ""

# --- Loop: keep tunnel alive across transient drops ---
$attempt = 0
while ($true) {
    $attempt++
    $stamp = Get-Date -Format "HH:mm:ss"
    Write-Host "[$stamp] attempt #$attempt — starting tunnel" -ForegroundColor DarkCyan
    & $Kubectl `
        --kubeconfig=$Kubeconfig `
        -n $Namespace `
        port-forward "pod/$Pod" "${LocalPort}:9092"
    $code = $LASTEXITCODE
    $stamp = Get-Date -Format "HH:mm:ss"
    Write-Host "[$stamp] tunnel exited with code $code, restarting in 3s..." -ForegroundColor Yellow
    Start-Sleep -Seconds 3
}
