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
    url = "https://api.vnstock.site/v2/stock/quote/history"
    
    # THÊM HEADERS ĐỂ "LÀM QUEN" VỚI SERVER
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Origin": "https://vnstock.site",
        "Referer": "https://vnstock.site/"
    }
    
    params = {
        "symbol": symbol,
        "from": "2024-01-01",
        "to": datetime.now().strftime('%Y-%m-%d'),
        "resolution": "1D",
        "type": "stock"
    }

    # SỬ DỤNG SESSION ĐỂ TRÁNH MAX RETRIES
    session = requests.Session()
    try:
        res = session.get(url, headers=headers, params=params, timeout=20)
        if res.status_code == 200:
            json_data = res.json()
            # Xử lý trường hợp dữ liệu trả về nằm trong key 'data'
            records = json_data.get('data', json_data)
            return pd.DataFrame(records)
        else:
            print(f"⚠️ Server trả lỗi {res.status_code} cho {symbol}")
            return None
    except Exception as e:
        print(f"❌ Lỗi kết nối khi lấy {symbol}: {str(e)}")
        return None

def run_etl_process():
    supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))
    token = os.getenv("VNSTOCK_TOKEN")
    symbols = ["TCB", "MSN", "VHM"]
    
    for symbol in symbols:
        try:
            print(f"🚀 SkillVault đang 'thông quan' dữ liệu cho {symbol}...")
            df = fetch_silver_data(symbol, token)
            
            if df is not None and not df.empty:
                df['symbol'] = symbol
                df['fetch_timestamp'] = datetime.utcnow().isoformat()
                # Ép kiểu dữ liệu để tránh lỗi JSON của Supabase
                data = df.where(pd.notnull(df), None).to_dict(orient='records')
                supabase.table("market_data").upsert(data, on_conflict="symbol,date").execute()
                print(f"✅ Đã đổ dữ liệu {symbol} vào kho thành công.")
            else:
                print(f"⚠️ Dữ liệu {symbol} trống, bỏ qua.")
        except Exception as e:
            print(f"❌ Lỗi xử lý tại {symbol}: {e}")

@app.get("/api/health")
def health():
    return {"status": "SkillVault v3.9.Final - Hardened", "timestamp": datetime.utcnow().isoformat()}

@app.post("/api/etl/run")
def trigger_etl(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_etl_process)
    return {"message": "Dòng chảy đã được gia cố và bắt đầu chạy."}
