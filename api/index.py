import os
from fastapi import FastAPI
from supabase import create_client
from vnstock_data import vnstock_data # Import chuẩn theo link anh Thịnh gửi
from dotenv import load_dotenv

load_dotenv()
app = FastAPI()

@app.get("/api/health")
def health():
    return {"status": "SkillVault v3.9.Final", "auth": "Member Verified"}

@app.get("/api/etl/run")
def trigger_etl():
    # Máy bơm sẽ tự động dùng VNSTOCK_API_KEY từ Environment Variable
    try:
        df = vnstock_data.stock_historical_data(
            symbol="TCB", 
            start_date='2024-01-01', 
            end_date='2026-04-23',
            resolution='1D', 
            type='stock'
        )
        return {"msg": "Dữ liệu đã về!", "count": len(df)}
    except Exception as e:
        return {"error": str(e)}
