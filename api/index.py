import os
import pandas as pd
from datetime import datetime
from fastapi import FastAPI, Query
from supabase import create_client
from dotenv import load_dotenv
import time

# Đổi sang import từ vnstock_data theo chuẩn mới của Member
try:
    from vnstock_data import vnstock_data
except ImportError:
    # Fallback dự phòng
    import vnstock_data

load_dotenv()
app = FastAPI()

@app.get("/api/health")
def health():
    return {"status": "SkillVault v3.9.Final", "engine": "Vnstock-Data 3.1.2 Member"}

@app.get("/api/etl/run")
def trigger_etl(s: str = Query(None)):
    supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))
    
    target_symbols = [item.strip().upper() for item in s.split(",")] if s else ["TCB", "MSN", "VHM", "HPG"]

    results = []
    failed = {}

    for symbol in target_symbols:
        try:
            # Sử dụng hàm historical data từ gói vnstock_data
            df = vnstock_data.stock_historical_data(
                symbol=symbol, 
                start_date='2024-01-01', 
                end_date=datetime.now().strftime('%Y-%m-%d'),
                resolution='1D', 
                type='stock'
            )

            if df is not None and not df.empty:
                # Đồng bộ tên cột
                if 'time' in df.columns:
                    df = df.rename(columns={'time': 'date'})
                
                df['symbol'] = symbol
                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
                df['fetch_timestamp'] = datetime.utcnow().isoformat()

                data = df.where(pd.notnull(df), None).to_dict(orient='records')
                supabase.table("market_data").upsert(data, on_conflict="symbol,date").execute()
                
                results.append(symbol)
                time.sleep(0.5)
            else:
                failed[symbol] = "Source returned empty"

        except Exception as e:
            failed[symbol] = str(e)

    return {
        "status": "Success",
        "updated": results,
        "failed": failed,
        "total": len(results)
    }
