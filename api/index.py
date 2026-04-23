import os
import requests
import pandas as pd
from datetime import datetime
from fastapi import FastAPI
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
app = FastAPI()

def fetch_data(symbol, token):
    url = "https://api.vnstock.site/v2/stock/quote/history"
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SkillVault/3.9"
    }
    params = {
        "symbol": symbol,
        "from": "2024-01-01",
        "to": datetime.now().strftime('%Y-%m-%d'),
        "resolution": "1D",
        "type": "stock"
    }
    try:
        res = requests.get(url, headers=headers, params=params, timeout=20)
        # LOG ĐỂ KIỂM SOÁT TẠI VERCEL
        print(f"🔍 Kiểm tra {symbol}: Status {res.status_code}")
        
        if res.status_code == 200:
            json_data = res.json()
            # In một phần dữ liệu để xem cấu trúc thực tế
            print(f"📦 Dữ liệu thô {symbol}: {str(json_data)[:100]}...")
            
            # Kiểm tra các trường hợp key khác nhau
            data = json_data.get('data', json_data)
            return pd.DataFrame(data)
        else:
            print(f"❌ Server Vnstock báo lỗi: {res.text}")
            return None
    except Exception as e:
        print(f"💥 Lỗi kết nối: {str(e)}")
        return None

@app.get("/api/health")
def health():
    return {"status": "SkillVault v3.9.Debug"}

@app.get("/api/etl/run")
def trigger_etl():
    supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))
    token = os.getenv("VNSTOCK_TOKEN")
    
    # Kiểm tra Token có tồn tại không
    if not token:
        return {"error": "Thiếu VNSTOCK_TOKEN trong Environment Variables"}
        
    results = []
    for s in ["TCB", "MSN", "VHM"]:
        df = fetch_data(s, token)
        if df is not None and not df.empty:
            df['symbol'] = s
            # Chuyển đổi tên cột nếu cần (Vnstock v2 đôi khi dùng tên khác)
            clean_records = df.where(pd.notnull(df), None).to_dict(orient='records')
            supabase.table("market_data").upsert(clean_records, on_conflict="symbol,date").execute()
            results.append(s)
    
    return {"message": "Kết quả chạy thử nghiệm", "updated": results, "token_check": "OK" if token else "Empty"}
