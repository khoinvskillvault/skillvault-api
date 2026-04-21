from fastapi import FastAPI
from vnstock3 import Vnstock
import pandas as pd

app = FastAPI()

@app.get("/api/stock/{symbol}")
def get_stock(symbol: str):
    try:
        # Chuyển mã về chữ hoa
        symbol = symbol.upper()
        stock = Vnstock().stock(symbol=symbol, source='TCBS') # Đổi sang TCBS cho ổn định
        
        # Lấy bảng giá
        df = stock.trading.price_board()
        
        if df is not None and not df.empty:
            # Lấy giá khớp lệnh (matchPrice)
            price = df.iloc[0].get('matchPrice', 0)
            return {
                "symbol": symbol, 
                "price": float(price), 
                "status": "success",
                "message": "Data fetched from TCBS"
            }
        else:
            return {"symbol": symbol, "error": "No data found", "status": "error"}
            
    except Exception as e:
        return {"symbol": symbol, "error": str(e), "status": "error"}
