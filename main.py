import requests
from fastapi import FastAPI

app = FastAPI()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
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
        url = f"https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/t/{sym}"
        r = requests.get(url, headers=HEADERS, timeout=10)
        data = r.json()
        return {
            "symbol": sym,
            "status": "success",
            "data": data
        }
    except Exception as e:
        return {"symbol": sym, "error": str(e), "status": "error"}
