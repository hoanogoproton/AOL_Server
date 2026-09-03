from main import app, CONFIG
import uvicorn
import os

if __name__ == "__main__":
    # Ưu tiên env AI_HOST/AI_PORT, nếu không có thì lấy section 'server'
    # trong config.yaml -> port luôn khớp với webhook/loopback của agent.
    server_cfg = CONFIG.get("server") or {}

    host = os.environ.get("AI_HOST", str(server_cfg.get("host", "0.0.0.0")))
    port = int(os.environ.get("AI_PORT", server_cfg.get("port", 8080)))

    # Bắt buộc workers=1: agent + server dùng chung trạng thái trong process.
    uvicorn.run(app, host=host, port=port, workers=1)