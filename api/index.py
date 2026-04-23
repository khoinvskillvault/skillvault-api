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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    params = {"symbol": symbol, "from": "2024-01-01", "to": datetime.now().strftime('%Y-%m-%d'), "resolution": "1D", "type": "stock"}
    
    with requests.Session() as s:
        res = s.get(url, headers=headers, params=params, timeout=20)
        return pd.DataFrame(res.json().get('data', [])) if res.status_code == 200 else None

def run_etl():
    supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))
    token = os.getenv("VNSTOCK_TOKEN")
    for s in ["TCB", "MSN", "VHM"]:
        df = fetch_data(s, token)
        if df is not None and not df.empty:
            df['symbol'] = s
            data = df.where(pd.notnull(df), None).to_dict(orient='records')
            supabase.table("market_data").upsert(data, on_conflict="symbol,date").execute()

@app.get("/api/health")
def health(): return {"status": "SkillVault Clean Start"}

@app.post("/api/etl/run")
def trigger(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_etl)
    return {"message": "Dòng chảy đã bắt đầu tái thiết."}
