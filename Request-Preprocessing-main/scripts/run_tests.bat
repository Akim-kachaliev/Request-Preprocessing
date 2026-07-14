@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

REM ============================================
REM  run_tests.bat — запуск функциональных и
REM  нагрузочных тестов системы ранжирования
REM ============================================

if "%API_URL%"=="" set API_URL=http://localhost:8000

echo ============================================
echo  Запуск тестов системы ранжирования
echo  API: %API_URL%
echo ============================================
echo.

python "%~dp0test_runner.py"

if %ERRORLEVEL% equ 0 (
    echo.
    echo Все тесты пройдены успешно.
) else (
    echo.
    echo Некоторые тесты завершились с ошибкой.
    exit /b 1
)

endlocal
pause