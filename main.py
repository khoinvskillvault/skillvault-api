import requests
from fastapi import FastAPI
from datetime import datetime

app = FastAPI()
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SkillVault/1.0)"}


@app.get("/")
def home():
    return {"message": "SkillVault API is Active", "status": "ok"}


@app.get("/api/stock/{symbol}")
def get_stock(symbol: str):
    """Get real-time stock price"""
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


@app.get("/api/stock/{symbol}/history")
def get_stock_history(symbol: str, range: str = "1y"):
    """
    Get historical OHLCV data (252 days for 1y)
    Usage: /api/stock/TCB/history?range=1y
    """
    sym = symbol.upper()
    try:
        # Parse range
        range_map = {"1y": 252, "6m": 126, "3m": 63, "1m": 21}
        if range not in range_map:
            return {"error": "Invalid range. Use: 1y, 6m, 3m, 1m", "status": "error"}
        
        days_back = range_map[range]
        
        # Fetch from Yahoo Finance
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}.VN?interval=1d&range={range}"
        r = requests.get(url, headers=HEADERS, timeout=10)
        raw = r.json()
        result = raw.get("chart", {}).get("result", [])
        
        if not result:
            return {"symbol": sym, "status": "error", "error": "No data from Yahoo"}
        
        timestamps = result[0].get("timestamp", [])
        quotes = result[0].get("indicators", {}).get("quote", [{}])[0]
        opens = quotes.get("open", [])
        highs = quotes.get("high", [])
        lows = quotes.get("low", [])
        closes = quotes.get("close", [])
        volumes = quotes.get("volume", [])
        
        # Build history list
        history_list = []
        for i, ts in enumerate(timestamps[-days_back:]):
            from datetime import datetime as dt
            date_str = dt.fromtimestamp(ts).strftime("%Y-%m-%d")
            
            history_list.append({
                "date": date_str,
                "open": round(opens[i], 2) if i < len(opens) and opens[i] is not None else None,
                "high": round(highs[i], 2) if i < len(highs) and highs[i] is not None else None,
                "low": round(lows[i], 2) if i < len(lows) and lows[i] is not None else None,
                "close": round(closes[i], 2) if i < len(closes) and closes[i] is not None else None,
                "volume": int(volumes[i]) if i < len(volumes) and volumes[i] is not None else 0
            })
        
        return {
            "symbol": sym,
            "range": range,
            "count": len(history_list),
            "data": history_list,
            "status": "success"
        }
    
    except Exception as e:
        return {"symbol": sym, "error": str(e), "status": "error"}


@app.get("/api/stock/{symbol}/fundamentals")
def get_stock_fundamentals(symbol: str):
    """
    Get fundamental ratios (P/E, P/B, ROE, etc) using TCBS API
    Usage: /api/stock/TCB/fundamentals
    """
    sym = symbol.upper()
    try:
        # TCBS API endpoint for stock ratios
        url = f"https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/{sym}/overview"
        r = requests.get(url, headers=HEADERS, timeout=10)
        
        if r.status_code != 200:
            # Fallback: Try vnstock endpoint
            url = f"https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/{sym}/fundamental"
            r = requests.get(url, headers=HEADERS, timeout=10)
        
        raw = r.json()
        
        # Extract data from TCBS
        if "stockOverview" in raw:
            data = raw.get("stockOverview", {})
        else:
            data = raw
        
        # Map TCBS fields to our format
        return {
            "symbol": sym,
            "pe_ratio": data.get("pe") or data.get("peRatio"),
            "pb_ratio": data.get("pb") or data.get("pbRatio"),
            "roe": data.get("roe") or data.get("returnOnEquity"),
            "roa": data.get("roa") or data.get("returnOnAssets"),
            "dividend_yield": data.get("dividendYield") or data.get("divyield"),
            "market_cap": data.get("marketCap"),
            "eps": data.get("eps"),
            "revenue_ttm": data.get("revenueIn4Quarter"),
            "profit_ttm": data.get("profitIn4Quarter"),
            "debt_to_equity": data.get("debtToEquity") or data.get("debtEquityRatio"),
            "current_price": data.get("currentPrice") or data.get("price"),
            "status": "success"
        }
    
    except Exception as e:
        return {
            "symbol": sym,
            "error": str(e),
            "status": "error",
            "note": "TCBS API returned no data. This is common for less-liquid stocks."
        }
