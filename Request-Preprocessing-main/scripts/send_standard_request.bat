@echo off
setlocal
if "%API_URL%"=="" set API_URL=http://localhost:8000
curl.exe -s -X POST "%API_URL%/rank_standard" ^
  -H "Content-Type: application/json" ^
  --data "@%~dp0payloads\standard_request.json"

pause