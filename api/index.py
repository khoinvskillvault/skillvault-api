#!/usr/bin/env python3
# ═══════════════════════════════════════════════════════════════════════════════
# SKILLVAULT PHASE 1 - Vercel Serverless (api/index.py) - WITH DEBUG LOGGING
# ═══════════════════════════════════════════════════════════════════════════════

import os
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
import pandas as pd
from fastapi import FastAPI, BackgroundTasks
from supabase import create_client, Client
from vnstock import Vnstock
from dotenv import load_dotenv

load_dotenv()

# Cấu hình hệ thống
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SYMBOLS = ["TCB", "MSN", "VHM"] # Tập trung vào các mã trọng tâm của anh
FETCH_PERIOD = "6m" # Tối ưu tốc độ cho Vercel (10s timeout)

app = FastAPI(title="SkillVault V3.9 - Production")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SkillVaultCore:
    def __init__(self):
        self.supabase: Optional[Client] = None
        self.vn: Optional[Vnstock] = None
        self.is_ready = False

    def initialize(self):
        if self.is_ready: return
        try:
            self.supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            self.vn = Vnstock()
            # Kích hoạt log nội bộ của vnstock để giám sát trên Vercel
            self.vn.show_log = True 
            self.is_ready = True
            logger.info("✅ SkillVault Core Initialized")
        except Exception as e:
            logger.error(f"❌ Init Failed: {str(e)}")

core = SkillVaultCore()

# --- HELPER: XỬ LÝ DỮ LIỆU SẠCH ---
def clean_data(df: pd.DataFrame, symbol: str) -> List[Dict]:
    if df is None or df.empty: return []
    df = df.copy()
    df['symbol'] = symbol
    df['fetch_timestamp'] = datetime.utcnow().isoformat()
    # Chuyển đổi kiểu dữ liệu để Supabase không báo lỗi JSON
    return df.where(pd.notnull(df), None).to_dict(orient='records')

# --- ETL LOGIC ---
def run_etl_process():
    core.initialize()
    if not core.is_ready: return
    
    for symbol in SYMBOLS:
        try:
            stock = core.vn.stock(symbol=symbol)
            
            # 1. Market Data
            df_price = stock.quote.history(period=FETCH_PERIOD)
            market_records = clean_data(df_price, symbol)
            if market_records:
                core.supabase.table("market_data").upsert(
                    market_records, on_conflict="symbol,date"
                ).execute()
                logger.info(f"📊 Upserted Market Data for {symbol}")

            # 2. Financial Ratios (Dành cho Ban Kiểm soát)
            df_ratio = stock.finance.ratio(period='year')
            ratio_records = clean_data(df_ratio, symbol)
            if ratio_records:
                core.supabase.table("fundamentals").upsert(
                    ratio_records, on_conflict="symbol,fiscal_year"
                ).execute()
                logger.info(f"💰 Upserted Fundamentals for {symbol}")

        except Exception as e:
            logger.error(f"⚠️ Error processing {symbol}: {str(e)}")

# --- ENDPOINTS ---
@app.get("/api/health")
def health():
    return {"status": "active", "timestamp": datetime.utcnow().isoformat()}

@app.post("/api/etl/run")
def trigger_etl(background_tasks: BackgroundTasks):
    # Dùng BackgroundTasks để vượt qua giới hạn 10s của Vercel
    background_tasks.add_task(run_etl_process)
    return {"message": "ETL process started in background", "symbols": SYMBOLS}
