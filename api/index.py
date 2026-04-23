import os
import pandas as pd
from datetime import datetime
from fastapi import FastAPI, Query
from supabase import create_client
from dotenv import load_dotenv
import time

# Import an toàn cho bản 3.1.2
try:
    from vnstock import vnstock_data
except ImportError:
    # Phòng trường hợp tên package trên server là vnstock_data
    import vnstock_data

load_dotenv()
app = FastAPI()

@app.get("/api/health")
def health():
    return {"status": "SkillVault v3.9.Final", "engine": "Vnstock 3.1.2 Stable"}

@app.get("/api/etl/run")
def trigger_etl(s: str = Query(None)):
    supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))
    
    # Danh sách mã mặc định nếu anh không điền ?s=
    target_symbols = [item.strip().upper() for item in s.split(",")] if s else ["TCB", "MSN", "VHM", "HPG"]

    results = []
    failed = {}

    for symbol in target_symbols:
        try:
            # Gọi hàm theo chuẩn REST API mới của bản 3.1.2
            df = vnstock_data.stock_historical_data(
                symbol=symbol, 
                start_date='2024-01-01', 
                end_date=datetime.now().strftime('%Y-%m-%d'),
                resolution='1D', 
                type='stock'
            )

            if df is not None and not df.empty:
                # Chuẩn hóa cột (Vnstock 3.1.x dùng 'time' hoặc 'date')
                if 'time' in df.columns:
                    df = df.rename(columns={'time': 'date'})
                
                # Bổ sung thông tin kiểm soát
                df['symbol'] = symbol
                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
                df['fetch_timestamp'] = datetime.utcnow().isoformat()

                # Đẩy dữ liệu vào kho (Upsert để tránh trùng)
                data = df.where(pd.notnull(df), None).to_dict(orient='records')
                supabase.table("market_data").upsert(data, on_conflict="symbol,date").execute()
                
                results.append(symbol)
                time.sleep(0.5) # Nghỉ ngắn để tránh nghẽn
            else:
                failed[symbol] = "Data Empty from Source"

        except Exception as e:
            failed[symbol] = str(e)

    return {
        "status": "Process Completed",
        "updated": results,
        "failed": failed,
        "total_updated": len(results)
    }
