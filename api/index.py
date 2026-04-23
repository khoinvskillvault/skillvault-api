import os
import pandas as pd
from datetime import datetime
from fastapi import FastAPI, Query
from supabase import create_client
from vnstock import vnstock_data
from dotenv import load_dotenv
import time

load_dotenv()
app = FastAPI()

@app.get("/api/health")
def health():
    return {"status": "SkillVault v3.9.Final - Compatible with 3.1.3"}

@app.get("/api/etl/run")
def trigger_etl(s: str = Query(None)):
    # 1. Khởi tạo kết nối
    supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))
    
    # 2. Xác định danh sách mã cần lấy
    if s:
        target_symbols = [item.strip().upper() for item in s.split(",")]
    else:
        target_symbols = ["TCB", "MSN", "VHM", "HPG"] # Thêm HPG làm mã đối chứng

    results = []
    failed = {}

    for symbol in target_symbols:
        try:
            # 3. Lấy dữ liệu từ Vnstock 3.1.3 (Dùng REST API mới ngầm định)
            df = vnstock_data.stock_historical_data(
                symbol=symbol, 
                start_date='2024-01-01', 
                end_date=datetime.now().strftime('%Y-%m-%d'),
                resolution='1D', 
                type='stock'
            )

            if df is not None and not df.empty:
                # 4. Chuẩn hóa dữ liệu cho Supabase
                # Vnstock 3.1.3 thường trả về: time, open, high, low, close, volume
                # Chúng ta đổi tên 'time' thành 'date' để khớp với bảng của anh
                if 'time' in df.columns:
                    df = df.rename(columns={'time': 'date'})
                
                # Ép kiểu dữ liệu để đảm bảo an toàn
                df['symbol'] = symbol
                df['fetch_timestamp'] = datetime.utcnow().isoformat()
                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

                # Chuyển thành list dict để Upsert
                data = df.where(pd.notnull(df), None).to_dict(orient='records')
                
                # 5. Đổ vào kho
                supabase.table("market_data").upsert(data, on_conflict="symbol,date").execute()
                results.append(symbol)
                
                # Nghỉ 1 giây để "đi nhẹ nói khẽ" với server
                time.sleep(1)
            else:
                failed[symbol] = "Dữ liệu trống (Empty)"

        except Exception as e:
            failed[symbol] = f"Lỗi: {str(e)}"

    return {
        "message": "Nghiệm thu SkillVault v3.1.3",
        "updated_count": len(results),
        "updated_symbols": results,
        "failed_info": failed
    }
