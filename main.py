import os
# Lệnh quan trọng nhất để chạy được trên Vercel
os.environ['VNSTOCK_CONFIG_DIR'] = '/tmp'

from fastapi import FastAPI
from vnstock3 import Vnstock
import pandas as pd

app = FastAPI()

@app.get("/api/stock/{symbol}")
def get_stock(symbol: str):
    try:
        symbol = symbol.upper()
        # Dùng nguồn dữ liệu TCBS hoặc VCI
        stock = Vnstock().stock(symbol=symbol, source='TCBS')
        df = stock.trading.price_board()
        
        if df is not None and not df.empty:
            price = df.iloc[0].get('matchPrice', 0)
            return {"symbol": symbol, "price": float(price), "status": "success"}
        return {"symbol": symbol, "error": "No data", "status": "error"}
    except Exception as e:
        return {"symbol": symbol, "error": str(e), "status": "error"}

@app.get("/")
def home():
    return {"message": "SkillVault API is Active"}
