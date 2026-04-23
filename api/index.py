import os
import pandas as pd
from datetime import datetime
from fastapi import FastAPI, Query
from supabase import create_client
from dotenv import load_dotenv
import time

# Chỉnh đúng tên thư viện từ tài liệu anh vừa gửi
try:
    import vnstock_data
except ImportError:
    # Dự phòng nếu hệ thống yêu cầu gọi cụ thể hơn
    from vnstock_data import vnstock_data

load_dotenv()
app = FastAPI()

@app.get("/api/health")
def health():
    return {"status": "SkillVault v3.9.Final", "engine": "Vnstock-Data Member Certified"}

@app.get("/api/etl/run")
def trigger_etl(s: str = Query(None)):
    supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))
    
    target_symbols = [item.strip().upper() for item in s.split(",")] if s else ["TCB", "MSN", "VHM", "HPG"]
    results = []
    failed = {}

    for symbol in target_symbols:
        try:
            # Sử dụng hàm lấy dữ liệu từ gói chuẩn vnstock_data
            df = vnstock_data.stock_historical_data(
                symbol=symbol, 
                start_date='2024-01-01', 
                end_date=datetime.now().strftime('%Y-%m-%d'),
                resolution='1D', 
                type='stock'
            )

            if df is not None and not df.empty:
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
                failed[symbol] = "Empty data"
        except Exception as e:
            failed[symbol] = str(e)

    return {"status": "Success", "updated": results, "failed": failed}
