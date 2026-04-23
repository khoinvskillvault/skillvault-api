import os
import sys
from fastapi import FastAPI
from dotenv import load_dotenv

# --- LỆNH "DẪN DÒNG" ĐẶC BIỆT ---
# Tìm đường dẫn đến nơi chứa thư viện Member mà Thịnh vừa cài
path_to_member = os.path.join(os.getcwd(), "vnstock_lib", "lib", "python3.12", "site-packages")
if os.path.exists(path_to_member):
    sys.path.append(path_to_member)
# ------------------------------

load_dotenv()
app = FastAPI()

@app.get("/api/health")
def health():
    return {
        "status": "SkillVault v3.9.Final",
        "lib_check": "Đã kết nối đường ống" if os.path.exists(path_to_member) else "Chưa thấy đường ống"
    }

@app.get("/api/etl/run")
def trigger_etl():
    try:
        # Giờ Python sẽ thấy được vnstock_data
        from vnstock_data import vnstock_data
        
        df = vnstock_data.stock_historical_data(
            symbol="TCB", 
            start_date='2024-01-01', 
            end_date='2026-04-23',
            resolution='1D', 
            type='stock'
        )
        return {"msg": "Nước đã về kho Member!", "data_count": len(df)}
    except Exception as e:
        return {
            "msg": "Máy bơm vẫn kẹt",
            "error_detail": str(e),
            "check_path": path_to_member
        }
