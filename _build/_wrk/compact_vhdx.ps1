# Compact Docker VHDX
Write-Host "Stopping Docker and WSL..."
Stop-Process -Name "Docker Desktop" -Force -ErrorAction SilentlyContinue
wsl --shutdown
Start-Sleep -Seconds 5

Write-Host "Current VHDX size:"
$size = (Get-Item "C:\Users\wgadb\AppData\Local\Docker\wsl\disk\docker_data.vhdx").Length / 1GB
Write-Host "$size GB"

Write-Host "Creating diskpart script..."
$scriptPath = "$env:TEMP\compact_docker.txt"
@"
select vdisk file="C:\Users\wgadb\AppData\Local\Docker\wsl\disk\docker_data.vhdx"
compact vdisk
exit
"@ | Out-File -FilePath $scriptPath -Encoding ASCII

Write-Host "Running diskpart (this may take a few minutes)..."
diskpart /s $scriptPath

Write-Host "New VHDX size:"
$newSize = (Get-Item "C:\Users\wgadb\AppData\Local\Docker\wsl\disk\docker_data.vhdx").Length / 1GB
Write-Host "$newSize GB"
Write-Host "Saved: $([math]::Round($size - $newSize, 2)) GB"
