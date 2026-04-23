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

def fetch_data(symbol, token):
    url = "https://api.vnstock.site/v2/stock/quote/history"
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SkillVault/3.9",
        "Accept": "application/json"
    }
    params = {
        "symbol": symbol,
        "from": "2024-01-01",
        "to": datetime.now().strftime('%Y-%m-%d'),
        "resolution": "1D",
        "type": "stock"
    }
    with requests.Session() as s:
        try:
            res = s.get(url, headers=headers, params=params, timeout=20)
            if res.status_code == 200:
                data = res.json().get('data', [])
                return pd.DataFrame(data)
            return None
        except:
            return None

def run_etl():
    # Khởi tạo Supabase Client
    supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))
    token = os.getenv("VNSTOCK_TOKEN")
    
    for s in ["TCB", "MSN", "VHM"]:
        df = fetch_data(s, token)
        if df is not None and not df.empty:
            df['symbol'] = s
            # Làm sạch dữ liệu: Chuyển NaN thành None để Postgres chấp nhận
            clean_records = df.where(pd.notnull(df), None).to_dict(orient='records')
            supabase.table("market_data").upsert(clean_records, on_conflict="symbol,date").execute()

@app.get("/api/health")
def health():
    return {"status": "SkillVault v3.9 Final Ready"}

@app.post("/api/etl/run")
def trigger(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_etl)
    return {"message": "Dòng chảy SkillVault đã bắt đầu tái thiết."}
