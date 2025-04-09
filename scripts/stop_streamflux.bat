@echo off
echo Stopping StreamFlux components...

REM Find Python processes and terminate them
taskkill /f /im python.exe

echo StreamFlux components stopped.