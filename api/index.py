#!/usr/bin/env python3
# ═══════════════════════════════════════════════════════════════════════════════
# SKILLVAULT PHASE 1 - Vercel Serverless (api/index.py) - WITH DEBUG LOGGING
# ═══════════════════════════════════════════════════════════════════════════════

import os
import requests
import pandas as pd
from datetime import datetime
from fastapi import FastAPI, BackgroundTasks
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
app = FastAPI()

def fetch_silver_data(symbol, token):
    # Đây là đường ống trực tiếp, không thông qua hàm history() dễ lỗi của thư viện
    url = "https://api.vnstock.site/v2/stock/quote/history"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "symbol": symbol,
        "from": "2024-01-01",
        "to": datetime.now().strftime('%Y-%m-%d'),
        "resolution": "1D",
        "type": "stock"
    }
    res = requests.get(url, headers=headers, params=params, timeout=15)
    if res.status_code == 200:
        return pd.DataFrame(res.json()['data'] if 'data' in res.json() else res.json())
    return None

def run_etl_process():
    supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))
    token = os.getenv("VNSTOCK_TOKEN")
    symbols = ["TCB", "MSN", "VHM"]
    
    for symbol in symbols:
        try:
            print(f"🚀 SkillVault đang rút dữ liệu trực tiếp cho {symbol}...")
            df = fetch_silver_data(symbol, token)
            
            if df is not None and not df.empty:
                df['symbol'] = symbol
                df['fetch_timestamp'] = datetime.utcnow().isoformat()
                # Làm sạch dữ liệu để Supabase không từ chối
                data = df.where(pd.notnull(df), None).to_dict(orient='records')
                supabase.table("market_data").upsert(data, on_conflict="symbol,date").execute()
                print(f"✅ Đã đổ dữ liệu {symbol} vào nhà kho.")
            else:
                print(f"⚠️ Không lấy được dữ liệu cho {symbol}")
        except Exception as e:
            print(f"❌ Lỗi tại {symbol}: {e}")

@app.get("/api/health")
def health():
    return {"status": "SkillVault v3.9.Final Ready", "timestamp": datetime.utcnow().isoformat()}

@app.post("/api/etl/run")
def trigger_etl(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_etl_process)
    return {"message": "Dòng chảy trực tiếp đã bắt đầu.", "symbols": ["TCB", "MSN", "VHM"]}
