import os
# Ép thư viện vnstock dùng thư mục /tmp của Vercel để không bị lỗi quyền ghi
os.environ['VNSTOCK_CONFIG_DIR'] = '/tmp'

from fastapi import FastAPI
from vnstock3 import Vnstock

app = FastAPI()

@app.get("/api/stock/{symbol}")
def get_stock(symbol: str):
    try:
        symbol = symbol.upper()
        # Sử dụng nguồn dữ liệu TCBS rất ổn định
        stock = Vnstock().stock(symbol=symbol, source='TCBS')
        
        df = stock.trading.price_board()
        
        if df is not None and not df.empty:
            # Lấy giá khớp gần nhất
            price = df.iloc[0].get('matchPrice', 0)
            return {
                "symbol": symbol, 
                "price": float(price), 
                "status": "success"
            }
        else:
            return {"symbol": symbol, "error": "Khong tim thay du lieu", "status": "error"}
            
    except Exception as e:
        return {"symbol": symbol, "error": str(e), "status": "error"}

@app.get("/")
def home():
    return {"message": "SkillVault API is running. Use /api/stock/TCB to test."}
