@echo off
REM ----------------------------------------------------------------------
REM  start_demo.bat  --  one-click NIDSaaS prototype demo launcher (Windows)
REM
REM  Opens 2 WSL terminals:
REM    Terminal A: docker compose logs -f (services pipeline)
REM    Terminal B: bash scripts/demo_pcap.sh
REM
REM  Pre-conditions:
REM    1. Docker Engine running in WSL (or Docker Desktop with WSL backend)
REM    2. Stack already booted with `docker compose up -d`
REM    3. /tmp/demo_attack.pcap exists in WSL (auto-sliced if missing)
REM ----------------------------------------------------------------------

REM Adjust this path if your repo lives elsewhere
set REPO=/mnt/c/Users/user/Downloads/NIDSaaS_Experiment/prototype

echo.
echo  ============================================
echo   NIDSaaS prototype demo launcher
echo  ============================================
echo.
echo   Step 1: Opening Terminal A (live logs)...
start "NIDSaaS logs"   wt -p "Ubuntu" -d "%REPO%" wsl -- bash -c "cd %REPO% && docker compose logs -f --tail=0 gateway flow_extractor snort_sidecar detector alert_fanout webhook_receiver"

REM wait 2 seconds so terminal A is up first
timeout /t 2 /nobreak >nul

echo   Step 2: Opening Terminal B (demo runner)...
start "NIDSaaS demo"   wt -p "Ubuntu" -d "%REPO%" wsl -- bash -c "cd %REPO% && bash scripts/demo_pcap.sh; echo; echo Press any key to close...; read -n 1"

echo.
echo   ✓ Both terminals opened.
echo   Watch Terminal A for live logs while Terminal B runs the demo.
echo.
pause
