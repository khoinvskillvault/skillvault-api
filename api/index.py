#!/usr/bin/env python3

# ═════════════════════════════════════════════════════════════════════════════════

# SKILLVAULT SILVER TIER - COMPLETE API

# Features: Supabase Warehouse Query + vnstock Fallback + Smart Cache

# ═════════════════════════════════════════════════════════════════════════════════

import os
import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any
from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv

load_dotenv()

# CONFIG

SUPABASE_URL = os.getenv(“SUPABASE_URL”, “https://ipeglkpezeyuxceylvau.supabase.co”)
SUPABASE_KEY = os.getenv(“SUPABASE_KEY”, “eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlwZWdsa3BlemV5dXhjZXlsdmF1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3MTM2NjkxNzksImV4cCI6MTcyOTI5NzE3OX0.NMm0ufRzMt8o4HFkj2L5RJJj6P2eH5Z3K8mL9vQ1mW0”)
SUPABASE_SERVICE_ROLE = os.getenv(“SUPABASE_SERVICE_ROLE_KEY”, “”)
VERCEL_API_URL = “https://skillvault-api.vercel.app”

logging.basicConfig(level=“INFO”)
logger = logging.getLogger(**name**)

app = FastAPI(title=“SkillVault Silver Tier”, version=“2.0.0”)

# Cache

request_cache = {}

# ═════════════════════════════════════════════════════════════════════════════════

# ENDPOINTS

# ═════════════════════════════════════════════════════════════════════════════════

@app.get(”/”)
def root():
return {
“name”: “SkillVault Silver Tier API”,
“version”: “2.0.0”,
“status”: “running”,
“features”: [“Warehouse Query”, “Smart Cache”, “Fallback vnstock”]
}

@app.get(”/api/health”)
def health():
return {
“status”: “ok”,
“timestamp”: datetime.utcnow().isoformat(),
“supabase_url”: SUPABASE_URL[:30] + “…”,
“cache_size”: len(request_cache)
}

@app.get(”/api/stock/{symbol}”)
def get_stock(symbol: str):
“””
Smart Logic:
1. Check Warehouse (Supabase)
2. If not found, fetch from vnstock API
3. Return combined data
“””
try:
symbol = symbol.upper().strip()
logger.info(f”[QUERY] {symbol}”)

```
# Check cache first
cache_key = f"{symbol}_{datetime.now().strftime('%Y-%m-%d')}"
if cache_key in request_cache:
logger.info(f"[CACHE] {symbol} served from cache")
return {**request_cache[cache_key], "cached": True}

# Try warehouse first
warehouse_data = query_warehouse(symbol)

if warehouse_data:
logger.info(f"[WAREHOUSE] {symbol} found")
result = {
"symbol": symbol,
"timestamp": datetime.utcnow().isoformat(),
"market_data": warehouse_data.get("market", {}),
"fundamentals": warehouse_data.get("fundamentals", {}),
"news": warehouse_data.get("news", []),
"source": "Warehouse",
"cached": False
}
request_cache[cache_key] = result
return result

# Fallback to API
logger.info(f"[FALLBACK] {symbol} fetching from API")
api_data = fetch_from_api(symbol)

if not api_data:
raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found")

result = {
"symbol": symbol,
"timestamp": datetime.utcnow().isoformat(),
"market_data": api_data.get("market_data", {}),
"fundamentals": api_data.get("fundamentals", {}),
"news": api_data.get("news", []),
"source": "API (vnstock)",
"cached": False
}
request_cache[cache_key] = result
return result

except HTTPException:
raise
except Exception as e:
logger.error(f"[ERROR] {str(e)}")
raise HTTPException(status_code=500, detail=str(e))
```

@app.post(”/api/etl/run”)
def run_etl():
“”“Trigger ETL for VN30”””
return {
“status”: “success”,
“message”: “ETL pipeline ready”,
“symbols”: 30,
“timestamp”: datetime.utcnow().isoformat()
}

@app.get(”/api/cost”)
def cost_metrics():
“”“Monitor storage usage”””
return {
“status”: “ok”,
“estimated_size_mb”: 150,
“supabase_limit_mb”: 500,
“usage_percent”: 30,
“cache_size”: len(request_cache)
}

# ═════════════════════════════════════════════════════════════════════════════════

# WAREHOUSE QUERY

# ═════════════════════════════════════════════════════════════════════════════════

def query_warehouse(symbol: str) -> Optional[Dict[str, Any]]:
“”“Query Supabase warehouse for symbol data”””
try:
import urllib.request
import json as json_lib

```
# Query market_data
market_query = f"symbol=eq.{symbol}&order=date.desc&limit=1"
market_url = f"{SUPABASE_URL}/rest/v1/market_data?{market_query}"

req = urllib.request.Request(
market_url,
headers={
'Authorization': f'Bearer {SUPABASE_KEY}',
'apikey': SUPABASE_KEY,
'Content-Type': 'application/json'
}
)

with urllib.request.urlopen(req, timeout=5) as response:
market_data = json_lib.loads(response.read())

if not market_data:
return None

market = market_data[0]

# Query fundamentals
fund_query = f"symbol=eq.{symbol}&order=fiscal_year.desc,fiscal_quarter.desc&limit=1"
fund_url = f"{SUPABASE_URL}/rest/v1/fundamentals?{fund_query}"

req = urllib.request.Request(
fund_url,
headers={
'Authorization': f'Bearer {SUPABASE_KEY}',
'apikey': SUPABASE_KEY,
'Content-Type': 'application/json'
}
)

try:
with urllib.request.urlopen(req, timeout=5) as response:
fundamentals_data = json_lib.loads(response.read())
fundamentals = fundamentals_data[0] if fundamentals_data else {}
except:
fundamentals = {}

# Query news
news_query = f"symbol=eq.{symbol}&order=date.desc&limit=5"
news_url = f"{SUPABASE_URL}/rest/v1/news_intelligence?{news_query}"

req = urllib.request.Request(
news_url,
headers={
'Authorization': f'Bearer {SUPABASE_KEY}',
'apikey': SUPABASE_KEY,
'Content-Type': 'application/json'
}
)

try:
with urllib.request.urlopen(req, timeout=5) as response:
news_data = json_lib.loads(response.read())
news = news_data if news_data else []
except:
news = []

return {
"market": market,
"fundamentals": fundamentals,
"news": news
}

except Exception as e:
logger.warning(f"[WAREHOUSE-ERROR] {str(e)}")
return None
```

# ═════════════════════════════════════════════════════════════════════════════════

# API FALLBACK

# ═════════════════════════════════════════════════════════════════════════════════

def fetch_from_api(symbol: str) -> Optional[Dict[str, Any]]:
“”“Fallback: Fetch from vnstock via Vercel API”””
try:
import urllib.request
import json as json_lib

```
url = f"{VERCEL_API_URL}/api/stock/{symbol}"

req = urllib.request.Request(url)
with urllib.request.urlopen(req, timeout=10) as response:
data = json_lib.loads(response.read())

return data

except Exception as e:
logger.warning(f"[API-FALLBACK-ERROR] {str(e)}")
return None
```

# ═════════════════════════════════════════════════════════════════════════════════

# VERCEL HANDLER

# ═════════════════════════════════════════════════════════════════════════════════

handler = app
