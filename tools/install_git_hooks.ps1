$ErrorActionPreference = "Stop"

git config core.hooksPath .githooks
Write-Host "Installed git hooks path: .githooks"
Write-Host "Pre-commit will run tools/ci_checks.py before each commit."
