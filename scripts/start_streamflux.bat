@echo off
echo Starting StreamFlux components...

REM Activate virtual environment
call venv\Scripts\activate.bat

REM Start data producers (in background)
start "Data Producers" cmd /c python -m data_producers.simulator_manager --sensors 5 --users 10 --symbols 20

REM Start Spark Streaming job (in background)
start "Spark Streaming" cmd /c python -m spark_processing.streaming_job

REM Start API server (in background)
start "API Server" cmd /c python -m api.flask_app

echo StreamFlux components started.
echo To stop them, close the windows or run stop_streamflux.bat