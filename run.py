from main import app
import uvicorn
import os

if __name__ == "__main__":
    host = os.environ.get("AI_HOST", "0.0.0.0")
    port = int(os.environ.get("AI_PORT", "8080"))
    uvicorn.run(app, host=host, port=port, workers=1)