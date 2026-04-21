import requests
from fastapi import FastAPI
from datetime import datetime, timedelta

app = FastAPI()

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Referer": "https://tcinvest.tcbs.com.vn/"
}

@app.get("/")
def home():
    return {"message": "SkillVault API is Active", "status": "ok"}

@app.get("/api/stock/{symbol}")
def get_stock(symbol: str):
    sym = symbol.upper()
    try:
        to_ts   = int(datetime.now().timestamp())
        from_ts = int((datetime.now() - timedelta(days=7)).timestamp())

        url = (f"https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/bar-time-series"
               f"?ticker={sym}&type=stock&resolution=D&from={from_ts}&to={to_ts}")

        r = requests.get(url, headers=HEADERS, timeout=10)
        return {
            "symbol": sym,
            "status": "success",
            "http_code": r.status_code,
            "data": r.json()
        }
    except Exception as e:
        return {"symbol": sym, "error": str(e), "status": "error"}
