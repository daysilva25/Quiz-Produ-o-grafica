@echo off
title Quiz de Producao Grafica - Serigrafia e Rotogravura
echo ======================================================================
echo   QUIZ MULTIPLAYER: SERIGRAFIA E ROTOGRAVURA
echo ======================================================================
cd /d "%~dp0"
start "" cmd /c "timeout /t 2 /nobreak >nul && start http://localhost:8000/host"
if exist "C:\Users\daysi\AppData\Local\Python\bin\python.exe" (
    C:\Users\daysi\AppData\Local\Python\bin\python.exe server.py
) else (
    python server.py
)
pause
