param(
  [switch]$BackendOnly,
  [switch]$FrontendOnly,
  [switch]$DryRun
)

$ErrorActionPreference = 'Stop'

if ($BackendOnly -and $FrontendOnly) {
  throw 'Usa solo uno tra -BackendOnly e -FrontendOnly.'
}

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$backendDir = Join-Path $projectRoot 'backend'
$frontendDir = Join-Path $projectRoot 'frontend'
$backendActivate = Join-Path $backendDir '.venv\Scripts\Activate.ps1'
$backendEnv = Join-Path $backendDir '.env'
$frontendEnv = Join-Path $frontendDir '.env'
$frontendNodeModules = Join-Path $frontendDir 'node_modules'

function Test-RequiredPath {
  param(
    [string]$Path,
    [string]$Label
  )

  if (-not (Test-Path -LiteralPath $Path)) {
    throw "$Label non trovato: $Path"
  }
}

function Start-DevWindow {
  param(
    [string]$Name,
    [string]$Command
  )

  if ($DryRun) {
    Write-Host ""
    Write-Host "[$Name]"
    Write-Host $Command
    return
  }

  Start-Process powershell.exe -ArgumentList @(
    '-NoExit',
    '-ExecutionPolicy', 'Bypass',
    '-Command', $Command
  ) | Out-Null
}

Test-RequiredPath -Path $backendDir -Label 'Cartella backend'
Test-RequiredPath -Path $frontendDir -Label 'Cartella frontend'

if (-not (Test-Path -LiteralPath $backendActivate)) {
  throw "Virtual environment backend non trovato: $backendActivate"
}

if (-not (Test-Path -LiteralPath $backendEnv)) {
  Write-Warning 'Manca backend/.env. Il backend potrebbe non partire senza le credenziali del database.'
}

if (-not (Test-Path -LiteralPath $frontendEnv)) {
  Write-Warning 'Manca frontend/.env. Controlla VITE_API_URL prima di avviare il frontend.'
}

if (-not (Test-Path -LiteralPath $frontendNodeModules)) {
  Write-Warning 'Manca frontend/node_modules. Esegui npm install in frontend prima di usare questo script.'
}

$backendCommand = @"
Set-Location '$backendDir'
& '$backendActivate'
uvicorn main:app --reload --host 0.0.0.0 --port 8000
"@

$frontendCommand = @"
Set-Location '$frontendDir'
npm run dev -- --host 0.0.0.0 --port 5173
"@

if (-not $FrontendOnly) {
  Start-DevWindow -Name 'backend' -Command $backendCommand
}

if (-not $BackendOnly) {
  Start-DevWindow -Name 'frontend' -Command $frontendCommand
}

if ($DryRun) {
  Write-Host ""
  Write-Host 'Dry run completato.'
  exit 0
}

Write-Host 'Backend e frontend avviati in due finestre PowerShell separate.'
Write-Host 'Frontend: http://127.0.0.1:5173'
Write-Host 'Backend:  http://127.0.0.1:8000'
