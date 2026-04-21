import os
os.environ['VNSTOCK_CONFIG_DIR'] = '/tmp'

from fastapi import FastAPI
from vnstock3 import Vnstock

app = FastAPI()

@app.get("/api/stock/{symbol}")
def get_stock(symbol: str):
    try:
        # Cách gọi dữ liệu trực tiếp, bỏ qua bước khởi tạo object phức tạp
        stock = Vnstock().stock(symbol=symbol.upper(), source='TCBS')
        price_df = stock.trading.price_board()
        
        if price_df is not None and not price_df.empty:
            current_price = price_df.iloc[0].get('matchPrice', 0)
            return {
                "symbol": symbol.upper(),
                "price": float(current_price),
                "status": "success"
            }
        return {"error": "No data", "status": "error"}
    except Exception as e:
        return {"error": str(e), "status": "error"}

@app.get("/")
def home():
    return {"message": "SkillVault API is Active"}
