@echo off
echo Stopping MongoDB...

REM Find MongoDB process and terminate it
taskkill /f /im mongod.exe

echo MongoDB stopped.