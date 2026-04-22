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
    Get fundamental ratios (P/E, P/B, ROE, etc)
    Usage: /api/stock/TCB/fundamentals
    """
    sym = symbol.upper()
    try:
        # Fetch from Yahoo Finance with params for detailed info
        url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{sym}.VN?modules=financialData,defaultKeyStatistics,summaryProfile"
        r = requests.get(url, headers=HEADERS, timeout=10)
        raw = r.json()
        
        result = raw.get("quoteSummary", {}).get("result", [{}])[0]
        financial_data = result.get("financialData", {})
        key_stats = result.get("defaultKeyStatistics", {})
        summary = result.get("summaryProfile", {})
        
        # Extract key ratios
        pe = financial_data.get("trailingPE", {}).get("raw")
        pb = key_stats.get("priceToBook", {}).get("raw")
        roe = financial_data.get("returnOnEquity", {}).get("raw")
        roa = financial_data.get("returnOnAssets", {}).get("raw")
        
        return {
            "symbol": sym,
            "pe_ratio": round(pe, 2) if pe else None,
            "pb_ratio": round(pb, 2) if pb else None,
            "roe": round(roe * 100, 2) if roe else None,
            "roa": round(roa * 100, 2) if roa else None,
            "dividend_yield": key_stats.get("trailingAnnualDividendYield", {}).get("raw"),
            "market_cap": financial_data.get("marketCap", {}).get("raw"),
            "eps": financial_data.get("trailingEps", {}).get("raw"),
            "revenue_ttm": financial_data.get("totalRevenue", {}).get("raw"),
            "profit_ttm": financial_data.get("totalDebt", {}).get("raw"),
            "debt_to_equity": financial_data.get("debtToEquity", {}).get("raw"),
            "status": "success"
        }
    
    except Exception as e:
        return {"symbol": sym, "error": str(e), "status": "error"}
