@echo off
echo Starting Zookeeper and Kafka...

set KAFKA_HOME=C:\kafka_2.13-3.4.0

REM Start Zookeeper
start "Zookeeper" cmd /c %KAFKA_HOME%\bin\windows\zookeeper-server-start.bat %KAFKA_HOME%\config\zookeeper.properties

REM Wait for Zookeeper to start
echo Waiting for Zookeeper to start...
timeout /t 10 /nobreak > nul

REM Start Kafka
start "Kafka" cmd /c %KAFKA_HOME%\bin\windows\kafka-server-start.bat %KAFKA_HOME%\config\server.properties

echo Kafka and Zookeeper started.
echo To stop them, close the windows or run stop_kafka.bat