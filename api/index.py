import os
import requests
import pandas as pd
import time
from datetime import datetime
from fastapi import FastAPI, Query
from supabase import create_client
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

load_dotenv()
app = FastAPI()

def get_secure_session():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

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
    
    session = get_secure_session()
    try:
        res = session.get(url, headers=headers, params=params, timeout=30)
        if res.status_code == 200:
            data = res.json().get('data', [])
            return pd.DataFrame(data)
        return None
    except Exception as e:
        print(f"❌ {symbol} - Lỗi: {str(e)}")
        return None

@app.get("/api/etl/run")
def trigger_etl(s: str = Query(None, description="Danh sách mã chứng khoán, cách nhau bởi dấu phẩy")):
    supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))
    token = os.getenv("VNSTOCK_TOKEN")
    
    # NẾU ANH KHÔNG ĐIỀN GÌ, NÓ SẼ LẤY 3 MÃ MẶC ĐỊNH
    if s:
        target_symbols = [item.strip().upper() for item in s.split(",")]
    else:
        target_symbols = ["TCB", "MSN", "VHM"]
        
    results = []
    for symbol in target_symbols:
        df = fetch_data(symbol, token)
        if df is not None and not df.empty:
            # Map cột và làm sạch dữ liệu
            column_map = {'t': 'date', 'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume'}
            df = df.rename(columns=column_map)
            
            df_final = df[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
            df_final['symbol'] = symbol
            df_final['fetch_timestamp'] = datetime.utcnow().isoformat()
            
            if df_final['date'].dtype != 'object':
                df_final['date'] = pd.to_datetime(df_final['date']).dt.strftime('%Y-%m-%d')

            data = df_final.where(pd.notnull(df_final), None).to_dict(orient='records')
            supabase.table("market_data").upsert(data, on_conflict="symbol,date").execute()
            results.append(symbol)
            time.sleep(1) # Nghỉ để tránh bị chặn IP
            
    return {
        "status": "Success",
        "message": f"Đã cập nhật xong {len(results)} mã",
        "symbols": results
    }
