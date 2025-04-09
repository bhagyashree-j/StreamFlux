@echo off
echo Starting MongoDB...

REM Customize these paths based on your MongoDB installation
set MONGODB_HOME=C:\Program Files\MongoDB\Server\6.0
set MONGODB_DATA=C:\data\db

REM Create data directory if it doesn't exist
if not exist "%MONGODB_DATA%" mkdir "%MONGODB_DATA%"

REM Start MongoDB
start "MongoDB" cmd /c "%MONGODB_HOME%\bin\mongod.exe" --dbpath="%MONGODB_DATA%"

echo MongoDB started.
echo To stop it, close the window or run stop_mongodb.bat