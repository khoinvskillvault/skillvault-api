from fastapi import FastAPI
from vnstock3 import Vnstock
import os

app = FastAPI()

@app.get("/api/stock/{symbol}")
def get_stock_data(symbol: str):
    # Khởi tạo vnstock với mã của bạn
    stock = Vnstock().stock(symbol=symbol, source='VCI') 
    
    # Lấy giá hiện tại và chỉ số cơ bản
    price_data = stock.trading.price_board()
    
    # Lấy chỉ số kỹ thuật (RSI, MA)
    ta_data = stock.trading.technicals(indicators=['rsi', 'ma'], periods={'ma': [20]})
    
    return {
        "symbol": symbol,
        "price": price_data.iloc[0]['matchPrice'],
        "rsi": ta_data.iloc[-1]['rsi'],
        "ma20": ta_data.iloc[-1]['ma20']
    }
