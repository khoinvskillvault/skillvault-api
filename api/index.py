import os
import sys
from fastapi import FastAPI
from dotenv import load_dotenv

# THÊM ĐOẠN NÀY: Ép Python tìm thư viện trong các thư mục tiềm năng
sys.path.append(os.path.join(os.getcwd(), ".venv", "lib", "python3.12", "site-packages"))
sys.path.append("/var/task")

load_dotenv()
app = FastAPI()

@app.get("/api/health")
def health():
    # Kiểm tra xem API Key đã vào chưa
    api_key_status = "Đã nhận" if os.getenv("VNSTOCK_API_KEY") else "Chưa có"
    return {
        "status": "SkillVault v3.9.Debug",
        "api_key": api_key_status,
        "msg": "Đang kiểm tra máy bơm..."
    }

@app.get("/api/etl/run")
def trigger_etl():
    try:
        # Kiểm tra xem có import được không
        import vnstock_data
        return {"msg": "Kết nối Vnstock Member thành công!"}
    except Exception as e:
        # Nếu lỗi, nó sẽ báo chính xác lỗi gì ở đây
        return {"msg": "Máy bơm vẫn kẹt", "error_detail": str(e)}
