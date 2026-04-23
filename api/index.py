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
        if res.status_code == 200:
            return pd.DataFrame(res.json().get('data', []))
        return None
    except:
        return None

@app.get("/api/health")
def health():
    return {"status": "SkillVault v3.9.Final - Synchronous"}

@app.get("/api/etl/run") # Đổi sang GET để anh dán link vào trình duyệt chạy cho tiện
def trigger_etl():
    supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))
    token = os.getenv("VNSTOCK_TOKEN")
    results = []
    
    for s in ["TCB", "MSN", "VHM"]:
        df = fetch_data(s, token)
        if df is not None and not df.empty:
            df['symbol'] = s
            clean_records = df.where(pd.notnull(df), None).to_dict(orient='records')
            supabase.table("market_data").upsert(clean_records, on_conflict="symbol,date").execute()
            results.append(s)
    
    return {"message": "Dòng chảy đã hoàn tất trực tiếp", "updated": results}
