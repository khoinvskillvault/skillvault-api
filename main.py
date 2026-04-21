import requests
from fastapi import FastAPI

app = FastAPI()

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SkillVault/1.0)"}

@app.get("/")
def home():
    return {"message": "SkillVault API is Active", "status": "ok"}

@app.get("/api/stock/{symbol}")
def get_stock(symbol: str):
    sym = symbol.upper()
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}.VN?interval=1d&range=5d"
        r = requests.get(url, headers=HEADERS, timeout=10)
        raw = r.json()

        result = raw.get("chart", {}).get("result", [])
        if not result:
            return {"symbol": sym, "status": "error", "error": "No data from Yahoo"}

        meta = result[0].get("meta", {})
        closes = result[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
        latest_close = next((v for v in reversed(closes) if v is not None), None)

        return {
            "symbol": sym,
            "status": "success",
            "price": latest_close,
            "currency": meta.get("currency"),
            "exchange": meta.get("exchangeName"),
            "regular_market_price": meta.get("regularMarketPrice"),
            "previous_close": meta.get("previousClose"),
        }
    except Exception as e:
        return {"symbol": sym, "error": str(e), "status": "error"}
