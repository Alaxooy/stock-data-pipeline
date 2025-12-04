# Git Configuration Script
# Run this script in VS Code's integrated terminal after Git is installed

Write-Host "Configuring Git user settings..." -ForegroundColor Cyan

git config --global user.name "Alaxooy"
git config --global user.email "alaxojoy007@gmail.com"

Write-Host ""
Write-Host "✅ Git configured successfully!" -ForegroundColor Green
Write-Host "   Name: Alaxooy" -ForegroundColor White
Write-Host "   Email: alaxojoy007@gmail.com" -ForegroundColor White
Write-Host ""
Write-Host "Verifying configuration..." -ForegroundColor Cyan
git config --global --list | Select-String "user"

