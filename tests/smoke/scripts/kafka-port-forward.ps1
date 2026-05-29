<#
.SYNOPSIS
  Start kubectl port-forward to ALL Kafka broker pods (test-stable) for local
  Step 7 of mt5-market-symbols-v2 smoke test.

.DESCRIPTION
  test-stable Kafka cluster has 3 brokers (kafka-controller-0/1/2). After the
  initial metadata exchange kafkajs follows advertised hostnames of EVERY
  broker, so a single-broker port-forward is not enough -- the client times
  out with `getaddrinfo ENOTFOUND kafka-controller-1.…`.

  This script:
  - Maps each broker's advertised hostname to a distinct 127.0.0.X address
    in the hosts file (interactive elevation if missing).
  - Spawns one kubectl port-forward per broker, each bound to its own
    127.0.0.X:9092, all running in background jobs.
  - Keeps tunnels alive (auto-restart on transient drops).

  Run in a SEPARATE PowerShell window before the test. Ctrl+C to stop.

.PARAMETER Namespace
  Kafka namespace. Default test-stable.

.PARAMETER Kubeconfig
  Path to kubeconfig. Default C:/Users/abolshakov/Documents/k8s/kubeconfig.

.PARAMETER Kubectl
  Path to kubectl binary. Default C:/Users/abolshakov/bin/kubectl.exe.
#>

[CmdletBinding()]
param(
    [string]$Namespace = "test-stable",
    [string]$Kubeconfig = "C:/Users/abolshakov/Documents/k8s/kubeconfig",
    [string]$Kubectl = "C:/Users/abolshakov/bin/kubectl.exe"
)

$ErrorActionPreference = "Stop"

$Brokers = @(
    @{ Pod = "kafka-controller-0"; Address = "127.0.0.1" },
    @{ Pod = "kafka-controller-1"; Address = "127.0.0.2" },
    @{ Pod = "kafka-controller-2"; Address = "127.0.0.3" }
)

$HostsPath = "$env:windir\System32\drivers\etc\hosts"

function Get-AdvertisedHost($pod) {
    return "$pod.kafka-controller-headless.$Namespace.svc.cluster.local"
}

function Test-HostsEntry($Hostname) {
    if (-not (Test-Path $HostsPath)) { return $false }
    $content = Get-Content -Raw $HostsPath -ErrorAction SilentlyContinue
    return $content -match [regex]::Escape($Hostname)
}

function Add-MissingHostsEntries {
    $missing = @()
    foreach ($b in $Brokers) {
        $h = Get-AdvertisedHost $b.Pod
        if (-not (Test-HostsEntry $h)) {
            $missing += "$($b.Address) $h"
        }
    }
    if ($missing.Count -eq 0) {
        Write-Host "All hosts entries present." -ForegroundColor Green
        return
    }
    Write-Host "Missing hosts entries:" -ForegroundColor Yellow
    $missing | ForEach-Object { Write-Host "  $_" -ForegroundColor Yellow }
    Write-Host "Elevating to add them (UAC prompt)..." -ForegroundColor Yellow

    $payload = $missing -join "`n"
    $cmd = "Add-Content -Path '$HostsPath' -Value '`n$payload' -Encoding ASCII"
    try {
        Start-Process powershell -ArgumentList "-NoProfile", "-Command", $cmd -Verb RunAs -Wait
    } catch {
        Write-Host "Failed to elevate. Add these lines manually to ${HostsPath}:" -ForegroundColor Red
        $missing | ForEach-Object { Write-Host "  $_" -ForegroundColor Cyan }
        exit 1
    }
    foreach ($b in $Brokers) {
        $h = Get-AdvertisedHost $b.Pod
        if (-not (Test-HostsEntry $h)) {
            Write-Host "Hosts entry still missing after elevation: $h. Aborting." -ForegroundColor Red
            exit 1
        }
    }
    Write-Host "Hosts entries added." -ForegroundColor Green
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

Add-MissingHostsEntries

Write-Host ""
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host " Kafka port-forward (3 brokers)" -ForegroundColor Cyan
foreach ($b in $Brokers) {
    Write-Host "  pod/$($b.Pod):9092 -> $($b.Address):9092" -ForegroundColor Cyan
}
Write-Host " For the test, run in another shell:" -ForegroundColor Cyan
Write-Host "   npx vitest run tests/smoke/mt5-market-orders.test.ts" -ForegroundColor Gray
Write-Host " Ctrl+C to stop." -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host ""

# Stop any leftover jobs from previous runs
Get-Job -Name "pf-kafka-*" -ErrorAction SilentlyContinue | Remove-Job -Force -ErrorAction SilentlyContinue

# Spawn one job per broker; each job loops with auto-restart
$jobs = @()
foreach ($b in $Brokers) {
    $jobs += Start-Job -Name "pf-kafka-$($b.Pod)" -ScriptBlock {
        param($pod, $address, $kubectl, $kubeconfig, $namespace)
        $attempt = 0
        while ($true) {
            $attempt++
            $stamp = Get-Date -Format "HH:mm:ss"
            Write-Output "[$stamp] $pod attempt #$attempt -- starting tunnel on $address:9092"
            & $kubectl `
                --kubeconfig=$kubeconfig `
                -n $namespace `
                port-forward "pod/$pod" "9092:9092" `
                --address $address
            $code = $LASTEXITCODE
            $stamp = Get-Date -Format "HH:mm:ss"
            Write-Output "[$stamp] $pod tunnel exited with code $code, restarting in 3s..."
            Start-Sleep -Seconds 3
        }
    } -ArgumentList $b.Pod, $b.Address, $Kubectl, $Kubeconfig, $Namespace
}

# Stream output from all jobs until Ctrl+C
try {
    while ($true) {
        foreach ($j in $jobs) {
            Receive-Job -Job $j | ForEach-Object {
                Write-Host "[$($j.Name)] $_" -ForegroundColor DarkCyan
            }
        }
        Start-Sleep -Milliseconds 500
    }
} finally {
    Write-Host "Stopping port-forward jobs..." -ForegroundColor Yellow
    $jobs | Stop-Job -ErrorAction SilentlyContinue
    $jobs | Remove-Job -Force -ErrorAction SilentlyContinue
}
