@echo off
echo Stopping Kafka and Zookeeper...

set KAFKA_HOME=C:\kafka_2.13-3.4.0

REM Stop Kafka
call %KAFKA_HOME%\bin\windows\kafka-server-stop.bat

REM Wait for Kafka to stop
timeout /t 5 /nobreak > nul

REM Stop Zookeeper
call %KAFKA_HOME%\bin\windows\zookeeper-server-stop.bat

echo Kafka and Zookeeper stopped.