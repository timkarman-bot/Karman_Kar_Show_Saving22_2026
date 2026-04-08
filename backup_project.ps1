$ErrorActionPreference = "Stop"

$projectRoot = "C:\karman-kar-shows"
$backupRoot = "C:\karman-kar-backups"
$timestamp = Get-Date -Format "yyyy-MM-dd_HHmmss"

$zipPath = Join-Path $backupRoot "karman-kar-shows_$timestamp.zip"
$dbSource = Join-Path $projectRoot "app.db"
$dbDest = Join-Path $backupRoot "app_$timestamp.db"

New-Item -ItemType Directory -Force -Path $backupRoot | Out-Null

if (Test-Path $zipPath) {
    Remove-Item $zipPath -Force
}

$itemsToZip = Get-ChildItem -Path $projectRoot -Force | Where-Object {
    $_.Name -notin @(".venv", "__pycache__", ".git")
}

Compress-Archive -Path $itemsToZip.FullName -DestinationPath $zipPath -Force

if (Test-Path $dbSource) {
    Copy-Item $dbSource $dbDest -Force
}

Write-Host ""
Write-Host "Backup complete."
Write-Host "ZIP: $zipPath"
if (Test-Path $dbSource) {
    Write-Host "DB : $dbDest"
} else {
    Write-Host "DB : app.db not found in project root, skipped."
}
Write-Host ""