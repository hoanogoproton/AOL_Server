uvicorn main:app --host 0.0.0.0 --port 8080 --workers 1

pyinstaller --onedir --console --name "AIInspectionServer" --add-data "static;static" --hidden-import "uvicorn.logging" --hidden-import "uvicorn.loops" --hidden-import "uvicorn.protocols" --hidden-import "uvicorn.protocols.http" --hidden-import "uvicorn.protocols.http.auto" --hidden-import "uvicorn.protocols.websocket" --hidden-import "uvicorn.middleware" --hidden-import "uvicorn.lifespan" --hidden-import "uvicorn.lifespan.on" --hidden-import "pydantic" --hidden-import "cv2" --hidden-import "serial" --hidden-import "yaml" --hidden-import "requests" run.py -y

curl -X POST "http:/172.17.164.14:8090/api/v1/reset/L"