uvicorn main:app --host 0.0.0.0 --port 8080 --workers 1

curl -X POST "http:/172.17.164.14:8090/api/v1/reset/L"