@echo off
REM Nitter Multi-Instance Management Script

if "%1"=="start" goto start
if "%1"=="stop" goto stop
if "%1"=="restart" goto restart
if "%1"=="status" goto status
if "%1"=="logs" goto logs
goto usage

:start
echo Starting Nitter multi-instances...
cd /d "%~dp0"

REM Stop any existing instances first
docker-compose -f docker-compose-multi.yml down

REM Create cache directories
if not exist cache1 mkdir cache1
if not exist cache2 mkdir cache2
if not exist cache3 mkdir cache3

REM Start multi-instances
docker-compose -f docker-compose-multi.yml up -d

echo Waiting for services to start...
timeout /t 10 /nobreak >nul

REM Check service status
echo Checking service status...
curl -f http://localhost:8080/settings >nul 2>&1 && echo ✅ Nitter1 (8080) - Running || echo ❌ Nitter1 (8080) - Failed to start
curl -f http://localhost:8081/settings >nul 2>&1 && echo ✅ Nitter2 (8081) - Running || echo ❌ Nitter2 (8081) - Failed to start
curl -f http://localhost:8082/settings >nul 2>&1 && echo ✅ Nitter3 (8082) - Running || echo ❌ Nitter3 (8082) - Failed to start
goto end

:stop
echo Stopping Nitter multi-instances...
cd /d "%~dp0"
docker-compose -f docker-compose-multi.yml down
goto end

:restart
echo Restarting Nitter multi-instances...
call %0 stop
timeout /t 5 /nobreak >nul
call %0 start
goto end

:status
echo Checking Nitter multi-instances status...
docker-compose -f docker-compose-multi.yml ps
echo.
echo Port checking:
curl -f http://localhost:8080/settings >nul 2>&1 && echo ✅ Nitter1 (8080) - Running || echo ❌ Nitter1 (8080) - Unreachable
curl -f http://localhost:8081/settings >nul 2>&1 && echo ✅ Nitter2 (8081) - Running || echo ❌ Nitter2 (8081) - Unreachable
curl -f http://localhost:8082/settings >nul 2>&1 && echo ✅ Nitter3 (8082) - Running || echo ❌ Nitter3 (8082) - Unreachable
goto end

:logs
echo Viewing logs...
cd /d "%~dp0"
docker-compose -f docker-compose-multi.yml logs -f
goto end

:usage
echo Usage: %0 {start^|stop^|restart^|status^|logs}
echo.
echo Command descriptions:
echo   start   - Start Nitter multi-instance services
echo   stop    - Stop Nitter multi-instance services
echo   restart - Restart Nitter multi-instance services
echo   status  - Check service status
echo   logs    - View service logs

:end