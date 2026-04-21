import os
# Ép hệ thống dùng thư mục tạm ngay lập tức
os.environ['VNSTOCK_CONFIG_DIR'] = '/tmp'

from fastapi import FastAPI
from vnstock3 import Vnstock

app = FastAPI()

@app.get("/api/stock/{symbol}")
def get_stock(symbol: str):
    try:
        # 1. Khởi tạo vnstock
        stock = Vnstock().stock(symbol=symbol.upper(), source='TCBS')
        
        # 2. Lấy bảng giá trực tiếp (Price Board)
        df = stock.trading.price_board()
        
        if df is not None and not df.empty:
            # Lấy giá khớp lệnh
            price = df.iloc[0].get('matchPrice', 0)
            return {
                "symbol": symbol.upper(),
                "price": float(price),
                "status": "success"
            }
        return {"error": "No data found", "status": "error"}
    except Exception as e:
        return {"error": str(e), "status": "error"}

@app.get("/")
def home():
    return {"message": "SkillVault API is Active"}
