#!/usr/bin/env python3

# ═════════════════════════════════════════════════════════════════════════════════

# SKILLVAULT SILVER TIER - ENHANCED API (main.py)

# Features: On-demand symbol queries, Smart caching, Parallel API calls

# Optimized for: Vercel Free (10s timeout), Supabase Free (500MB)

# ═════════════════════════════════════════════════════════════════════════════════

import os
import json
import logging
import asyncio
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor
import hashlib

import pandas as pd
import numpy as np
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

load_dotenv()

# ═════════════════════════════════════════════════════════════════════════════════

# CONFIGURATION

# ═════════════════════════════════════════════════════════════════════════════════

SUPABASE_URL = os.getenv(“SUPABASE_URL”)
SUPABASE_SERVICE_ROLE_KEY = os.getenv(“SUPABASE_SERVICE_ROLE_KEY”)
VNSTOCK_TOKEN = os.getenv(“VNSTOCK_TOKEN”, “”) # SILVER tier token
LOG_LEVEL = os.getenv(“LOG_LEVEL”, “INFO”)

# ⭐ VN30 (Scheduled daily fetch)

SYMBOLS_VN30 = [
“VHM”, “BID”, “VCB”, “FPT”, “MWG”, “VNM”, “SAB”, “TCB”, “TPB”, “ACB”,
“VIC”, “CTG”, “GAS”, “SHB”, “EIB”, “HDB”, “OCB”, “STB”, “NVL”, “VPB”,
“MSN”, “KDH”, “VJC”, “VNA”, “PNJ”, “DXG”, “REE”, “DIG”, “HVN”, “PDR”
]

# On-demand fetch settings

FETCH_PERIOD = “1y”
MAX_PARALLEL_FETCHES = 5 # Parallel API calls (avoid throttle)
CACHE_TTL_HOURS = 24 # Cache for 24 hours
MAX_CACHE_SIZE_SYMBOLS = 100 # Keep cache for top 100 symbols

CONFIDENCE_THRESHOLDS = {
“market_data”: 80,
“fundamentals”: 70,
“macro_data”: 75,
“foreign_flow”: 75,
“news_intelligence”: 60,
“market_insights”: 80
}

logging.basicConfig(level=LOG_LEVEL, format=’%(asctime)s [%(levelname)s] %(message)s’)
logger = logging.getLogger(**name**)

app = FastAPI(title=“SkillVault Silver API”, version=“2.0.0”)

# Global state (Lazy init)

supabase_client = None
vnstock_client = None
clients_initialized = False
init_error = None
request_cache = {} # Simple in-memory cache for symbol requests

# ═════════════════════════════════════════════════════════════════════════════════

# SECTION 1: INITIALIZATION (Lazy - No crash on startup)

# ═════════════════════════════════════════════════════════════════════════════════

def init_clients_lazy():
“”“Initialize clients on-demand (Lazy Loading)”””
global supabase_client, vnstock_client, clients_initialized, init_error

```
if clients_initialized:
return supabase_client, vnstock_client, init_error

clients_initialized = True

try:
if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
init_error = "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY"
logger.error(f"[INIT] {init_error}")
return None, None, init_error

# Init Supabase
try:
from supabase import create_client
supabase_client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
logger.info("[INIT] Supabase connected ✓")
except Exception as e:
init_error = f"Supabase init failed: {str(e)}"
logger.error(f"[INIT] {init_error}")
return None, None, init_error

# Init vnstock (with SILVER token if available)
try:
from vnstock import Vnstock
vnstock_client = Vnstock()
tier = "Silver" if VNSTOCK_TOKEN else "Free"
logger.info(f"[INIT] vnstock initialized ({tier} tier) ✓")
except Exception as e:
init_error = f"vnstock init failed: {str(e)}"
logger.error(f"[INIT] {init_error}")
return supabase_client, None, init_error

return supabase_client, vnstock_client, None

except Exception as e:
init_error = f"Unexpected init error: {str(e)}"
logger.error(f"[INIT] {init_error}")
return None, None, init_error
```

# ═════════════════════════════════════════════════════════════════════════════════

# SECTION 2: SMART CACHING LAYER

# ═════════════════════════════════════════════════════════════════════════════════

class SmartCache:
“”“Cache with TTL and LRU eviction”””

```
def __init__(self, ttl_hours=24, max_size=100):
self.cache = {}
self.timestamps = {}
self.ttl_hours = ttl_hours
self.max_size = max_size
self.access_count = {}

def get(self, key: str) -> Optional[Any]:
"""Get from cache if not expired"""
if key not in self.cache:
return None

# Check expiration
if datetime.now() - self.timestamps[key] > timedelta(hours=self.ttl_hours):
del self.cache[key]
del self.timestamps[key]
return None

self.access_count[key] = self.access_count.get(key, 0) + 1
return self.cache[key]

def set(self, key: str, value: Any) -> None:
"""Set cache value"""
# LRU eviction if cache full
if len(self.cache) >= self.max_size:
lru_key = min(self.access_count, key=self.access_count.get)
del self.cache[lru_key]
del self.timestamps[lru_key]
del self.access_count[lru_key]

self.cache[key] = value
self.timestamps[key] = datetime.now()
self.access_count[key] = 1

def clear_expired(self) -> int:
"""Remove expired entries"""
expired = [k for k, v in self.timestamps.items()
if datetime.now() - v > timedelta(hours=self.ttl_hours)]
for k in expired:
del self.cache[k]
del self.timestamps[k]
return len(expired)
```

cache = SmartCache(ttl_hours=CACHE_TTL_HOURS, max_size=MAX_CACHE_SIZE_SYMBOLS)

def should_fetch_fresh(symbol: str) -> bool:
“”“Check if we should fetch fresh data or use cache”””
if symbol in SYMBOLS_VN30:
return True # Always fetch fresh for VN30

```
# For others, check cache
cached = cache.get(f"{symbol}_market_data")
return cached is None
```

# ═════════════════════════════════════════════════════════════════════════════════

# SECTION 3: PARALLEL FETCH FUNCTIONS (SILVER TIER)

# ═════════════════════════════════════════════════════════════════════════════════

def fetch_market_data_single(vnstock_client, symbol: str) -> Optional[pd.DataFrame]:
“”“Fetch OHLCV + Foreign/Proprietary Flow for single symbol”””
try:
logger.info(f” [FETCH] {symbol} market data…”)

```
# OHLCV data
df = vnstock_client.stock(symbol=symbol).history(period=FETCH_PERIOD)

if len(df) == 0:
logger.warning(f" {symbol}: Empty OHLCV data")
return None

df['symbol'] = symbol
df['source'] = 'vnstock'
df['fetch_timestamp'] = datetime.utcnow().isoformat()
df['confidence_score'] = 100

# ⭐ SILVER TIER: Try to fetch Foreign Flow (if available)
try:
foreign_df = vnstock_client.stock(symbol=symbol).foreign_flow()
if len(foreign_df) > 0:
# Merge foreign flow data
foreign_df['date'] = pd.to_datetime(foreign_df['date']).dt.date
df = df.merge(foreign_df[['date', 'foreign_buy_vol', 'foreign_sell_vol', 'foreign_net_flow']],
on='date', how='left')
logger.info(f" ✓ {symbol}: Added foreign flow data")
except Exception as e:
logger.warning(f" {symbol}: Foreign flow unavailable ({str(e)[:50]})")

return df

except Exception as e:
logger.error(f" ✗ {symbol}: {str(e)[:100]}")
return None
```

def fetch_market_data_parallel(vnstock_client, symbols: List[str]) -> List[pd.DataFrame]:
“”“Fetch market data for multiple symbols in parallel”””
logger.info(f”[FETCH] market_data for {len(symbols)} symbols (parallel)…”)

```
market_data_list = []

with ThreadPoolExecutor(max_workers=MAX_PARALLEL_FETCHES) as executor:
futures = {
executor.submit(fetch_market_data_single, vnstock_client, symbol): symbol
for symbol in symbols
}

for future in futures:
try:
df = future.result(timeout=5)
if df is not None:
market_data_list.append(df)
symbol = futures[future]
cache.set(f"{symbol}_market_data", df) # Cache result
except Exception as e:
logger.warning(f" Parallel fetch timeout or error: {str(e)[:50]}")

logger.info(f"[FETCH] market_data: {len(market_data_list)}/{len(symbols)} symbols fetched")
return market_data_list
```

def fetch_fundamentals_single(vnstock_client, symbol: str) -> List[Dict[str, Any]]:
“”“Fetch fundamentals including ⭐ SILVER: Cash Flow + Debt/Equity”””
try:
logger.info(f” [FETCH] {symbol} fundamentals…”)

```
ratio_df = vnstock_client.stock(symbol=symbol).ratio()
income_df = vnstock_client.stock(symbol=symbol).income_statement()

# ⭐ SILVER TIER: Balance Sheet (new)
try:
balance_df = vnstock_client.stock(symbol=symbol).balance_sheet()
except:
balance_df = pd.DataFrame()
logger.warning(f" {symbol}: Balance sheet unavailable")

# ⭐ SILVER TIER: Cash Flow (new)
try:
cash_flow_df = vnstock_client.stock(symbol=symbol).cash_flow()
except:
cash_flow_df = pd.DataFrame()
logger.warning(f" {symbol}: Cash flow unavailable")

fundamentals_list = []

for idx, row in income_df.iterrows():
try:
publish_date = pd.to_datetime(row.get('date', datetime.now()))
fiscal_year = publish_date.year
fiscal_quarter = (publish_date.month - 1) // 3 + 1

# ⭐ Extract SILVER tier data
total_assets = None
current_assets = None
debt_st = None
debt_lt = None
debt_equity = None

if len(balance_df) > 0:
try:
total_assets = int(balance_df.iloc[0].get('total_assets', None)) if balance_df.iloc[0].get('total_assets') else None
current_assets = int(balance_df.iloc[0].get('current_assets', None)) if balance_df.iloc[0].get('current_assets') else None
debt_st = int(balance_df.iloc[0].get('short_term_debt', None)) if balance_df.iloc[0].get('short_term_debt') else None
debt_lt = int(balance_df.iloc[0].get('long_term_debt', None)) if balance_df.iloc[0].get('long_term_debt') else None
except:
pass

fcf = None
ocf = None
if len(cash_flow_df) > 0:
try:
ocf = int(cash_flow_df.iloc[0].get('operating_cf', None)) if cash_flow_df.iloc[0].get('operating_cf') else None
capex = int(cash_flow_df.iloc[0].get('capex', None)) if cash_flow_df.iloc[0].get('capex') else None
if ocf and capex:
fcf = ocf - capex
except:
pass

record = {
'symbol': symbol,
'fiscal_year': fiscal_year,
'fiscal_quarter': fiscal_quarter,
'pe_ratio': float(ratio_df.iloc[0].get('pe', None)) if len(ratio_df) > 0 else None,
'pb_ratio': float(ratio_df.iloc[0].get('pb', None)) if len(ratio_df) > 0 else None,
'roe': float(ratio_df.iloc[0].get('roe', None)) if len(ratio_df) > 0 else None,
'roa': float(ratio_df.iloc[0].get('roa', None)) if len(ratio_df) > 0 else None,
'eps': float(row.get('eps', None)) if row.get('eps') else None,
'dividend_yield': float(ratio_df.iloc[0].get('dividend_yield', None)) if len(ratio_df) > 0 else None,

# ⭐ SILVER TIER
'total_assets': total_assets,
'current_assets': current_assets,
'debt_st': debt_st,
'debt_lt': debt_lt,
'debt_equity': debt_equity if debt_equity else (float(ratio_df.iloc[0].get('debt_equity', None)) if len(ratio_df) > 0 else None),
'free_cash_flow': fcf,
'operating_cf': ocf,

'revenue_ttm': int(row.get('revenue', 0)) if row.get('revenue') else None,
'profit_ttm': int(row.get('profit', 0)) if row.get('profit') else None,
'publish_date': publish_date.strftime('%Y-%m-%d'),
'source': 'vnstock',
'fetch_timestamp': datetime.utcnow().isoformat(),
'confidence_score': 100
}
fundamentals_list.append(record)
except Exception as e:
logger.warning(f" Skip record: {str(e)[:50]}")

return fundamentals_list

except Exception as e:
logger.error(f" ✗ {symbol} fundamentals: {str(e)[:100]}")
return []
```

def fetch_fundamentals_parallel(vnstock_client, symbols: List[str]) -> List[Dict]:
“”“Fetch fundamentals for multiple symbols in parallel”””
logger.info(f”[FETCH] fundamentals for {len(symbols)} symbols (parallel)…”)

```
fundamentals_list = []

with ThreadPoolExecutor(max_workers=MAX_PARALLEL_FETCHES) as executor:
futures = [executor.submit(fetch_fundamentals_single, vnstock_client, symbol) for symbol in symbols]

for future in futures:
try:
records = future.result(timeout=5)
fundamentals_list.extend(records)
except Exception as e:
logger.warning(f" Parallel fetch timeout: {str(e)[:50]}")

logger.info(f"[FETCH] fundamentals: {len(fundamentals_list)} records")
return fundamentals_list
```

def fetch_macro_data(vnstock_client) -> List[Dict]:
“”“Fetch macro data (Tầng 0)”””
logger.info(”[FETCH] macro_data…”)
try:
macro_df = vnstock_client.macro()
macro_list = []

```
for idx, row in macro_df.iterrows():
try:
date_str = pd.to_datetime(row.get('date', datetime.now())).strftime('%Y-%m')
record = {
'date': date_str,
'period_type': 'MONTHLY',
'gdp_growth': float(row.get('gdp', None)) if row.get('gdp') else None,
'inflation_rate': float(row.get('cpi', None)) if row.get('cpi') else None,
'interest_rate': float(row.get('interest_rate', None)) if row.get('interest_rate') else None,
'usd_vnd_rate': float(row.get('usd_vnd', None)) if row.get('usd_vnd') else None,
'fdi_inflow': int(row.get('fdi', 0)) if row.get('fdi') else None,
'source': 'vnstock',
'fetch_timestamp': datetime.utcnow().isoformat(),
'confidence_score': 100
}
macro_list.append(record)
except:
pass

return macro_list
except Exception as e:
logger.warning(f"[FETCH] macro failed: {str(e)[:50]}")
return []
```

def fetch_news_data(vnstock_client, symbols: List[str]) -> List[Dict]:
“”“⭐ SILVER TIER: Fetch news from 21 official sponsors”””
logger.info(f”[FETCH] news_intelligence for {len(symbols)} symbols…”)

```
news_list = []

for symbol in symbols:
try:
news_df = vnstock_client.stock(symbol=symbol).news(limit=50) # ⭐ SILVER: Up to 21 sponsors

for idx, row in news_df.iterrows():
try:
record = {
'symbol': symbol,
'date': pd.to_datetime(row.get('publish_date', datetime.now())).strftime('%Y-%m-%d'),
'title': str(row.get('title', ''))[:200], # Trim to 200 chars (storage optimization)
'summary': str(row.get('description', ''))[:500] if row.get('description') else None,
'source': str(row.get('source', 'unknown')), # ⭐ Official sponsor source
'url': str(row.get('url', None)) if row.get('url') else None,
'news_type': 'general', # Will be categorized by Claude in Phase 3
'sentiment': None,
'sentiment_confidence': None,
'key_facts': None,
'source_credibility': 80, # Default for vnstock-sourced news
'fetch_timestamp': datetime.utcnow().isoformat(),
'confidence_score': 80
}
news_list.append(record)
except:
pass
except Exception as e:
logger.warning(f" {symbol} news: {str(e)[:50]}")

logger.info(f"[FETCH] news: {len(news_list)} articles")
return news_list
```

def fetch_market_insights(vnstock_client) -> List[Dict]:
“”“Fetch market summary (Tầng 1)”””
logger.info(”[FETCH] market_insights…”)
try:
vnindex_df = vnstock_client.stock(symbol=‘VNINDEX’).quote()

```
record = {
'date': datetime.now().strftime('%Y-%m-%d'),
'vnindex_close': float(vnindex_df.iloc[0].get('price', None)) if len(vnindex_df) > 0 else None,
'vnindex_change': float(vnindex_df.iloc[0].get('change', None)) if len(vnindex_df) > 0 else None,
'vnindex_change_percent': float(vnindex_df.iloc[0].get('change_percent', None)) if len(vnindex_df) > 0 else None,
'breadth_advance': None,
'breadth_decline': None,
'breadth_unchanged': None,
'top_gainer': None,
'top_gainer_change': None,
'top_loser': None,
'top_loser_change': None,
'vnindex_pe': None,
'vnindex_pb': None,
'total_liquidity': None,
'source': 'vnstock',
'fetch_timestamp': datetime.utcnow().isoformat(),
'confidence_score': 80
}

return [record]
except Exception as e:
logger.warning(f"[FETCH] market_insights: {str(e)[:50]}")
return []
```

# ═════════════════════════════════════════════════════════════════════════════════

# SECTION 4: CALCULATE TECHNICALS

# ═════════════════════════════════════════════════════════════════════════════════

def calculate_ma(prices: np.ndarray, window: int) -> np.ndarray:
if len(prices) < window:
return np.full_like(prices, np.nan, dtype=float)
ma = np.zeros_like(prices, dtype=float)
ma[:window-1] = np.nan
for i in range(window-1, len(prices)):
ma[i] = np.mean(prices[i-window+1:i+1])
return ma

def calculate_rsi(prices: np.ndarray, period: int = 14) -> np.ndarray:
if len(prices) < period + 1:
return np.full_like(prices, np.nan, dtype=float)
deltas = np.diff(prices)
gains = np.where(deltas > 0, deltas, 0)
losses = np.where(deltas < 0, -deltas, 0)
rsi = np.zeros_like(prices, dtype=float)
rsi[:period+1] = np.nan
for i in range(period, len(prices)-1):
avg_gain = np.mean(gains[i-period+1:i+1])
avg_loss = np.mean(losses[i-period+1:i+1])
if avg_loss == 0:
rsi[i+1] = 100.0 if avg_gain > 0 else 50.0
else:
rs = avg_gain / avg_loss
rsi[i+1] = 100.0 - (100.0 / (1.0 + rs))
return rsi

def calculate_technicals(market_data_list: List[pd.DataFrame]) -> List[pd.DataFrame]:
“”“Calculate technical indicators”””
logger.info(”[CALC] technicals…”)
enriched_list = []

```
for df in market_data_list:
try:
df = df.sort_values('date').reset_index(drop=True)
closes = df['close'].values.astype(float)

df['ma20'] = pd.Series(calculate_ma(closes, 20))
df['ma50'] = pd.Series(calculate_ma(closes, 50))
df['ma200'] = pd.Series(calculate_ma(closes, 200))
df['rsi'] = pd.Series(calculate_rsi(closes, 14))

lows = df['low'].values.astype(float)
highs = df['high'].values.astype(float)
df['support_level'] = np.nanmin(lows[~np.isnan(lows)])
df['resistance_level'] = np.nanmax(highs[~np.isnan(highs)])

enriched_list.append(df)
except Exception as e:
logger.warning(f" Calc error: {str(e)[:50]}")
enriched_list.append(df)

return enriched_list
```

# ═════════════════════════════════════════════════════════════════════════════════

# SECTION 5: VALIDATE & UPSERT

# ═════════════════════════════════════════════════════════════════════════════════

def validate_records(records: List[Dict], table_name: str, threshold: int) -> Tuple[List[Dict], int]:
“”“Validate records before upsert”””
logger.info(f”[VALIDATE] {table_name} ({len(records)} records)…”)

```
valid_records = []
skipped = 0

for record in records:
try:
non_null = sum(1 for v in record.values() if v is not None)
completeness = (non_null / len(record)) * 100

confidence = min(int(completeness / 10) * 10, 100)
record['confidence_score'] = min(record.get('confidence_score', 100), confidence)

if record.get('confidence_score', 0) >= threshold:
valid_records.append(record)
else:
skipped += 1
except:
skipped += 1

logger.info(f" Accepted: {len(valid_records)}, Skipped: {skipped}")
return valid_records, skipped
```

def dataframe_to_records(df: pd.DataFrame) -> List[Dict]:
“”“Convert DataFrame to list of dicts”””
records = []
for idx, row in df.iterrows():
record = row.dropna().to_dict()
for key, value in record.items():
if isinstance(value, (np.integer, np.floating)):
record[key] = float(value) if isinstance(value, np.floating) else int(value)
records.append(record)
return records

def upsert_to_supabase(supabase_client, market_data, fundamentals, macro, news, insights) -> Dict:
“”“Upsert data to Supabase with error handling”””
logger.info(”[UPSERT]…”)

```
stats = {
'market_data_inserted': 0,
'fundamentals_inserted': 0,
'macro_inserted': 0,
'news_inserted': 0,
'market_insights_inserted': 0,
'total_failed': 0
}

if not supabase_client:
return stats

try:
if market_data:
supabase_client.table("market_data").upsert(market_data, on_conflict="symbol,date").execute()
stats['market_data_inserted'] = len(market_data)
logger.info(f" ✓ market_data: {len(market_data)}")
except Exception as e:
logger.error(f" ✗ market_data: {str(e)[:100]}")
stats['total_failed'] += len(market_data)

try:
if fundamentals:
supabase_client.table("fundamentals").upsert(fundamentals, on_conflict="symbol,fiscal_year,fiscal_quarter").execute()
stats['fundamentals_inserted'] = len(fundamentals)
logger.info(f" ✓ fundamentals: {len(fundamentals)}")
except Exception as e:
logger.error(f" ✗ fundamentals: {str(e)[:100]}")
stats['total_failed'] += len(fundamentals)

try:
if macro:
supabase_client.table("macro_data").upsert(macro, on_conflict="date,period_type").execute()
stats['macro_inserted'] = len(macro)
logger.info(f" ✓ macro_data: {len(macro)}")
except Exception as e:
logger.error(f" ✗ macro_data: {str(e)[:100]}")
stats['total_failed'] += len(macro)

try:
if news:
supabase_client.table("news_intelligence").insert(news).execute()
stats['news_inserted'] = len(news)
logger.info(f" ✓ news_intelligence: {len(news)}")
except Exception as e:
logger.error(f" ✗ news_intelligence: {str(e)[:100]}")
stats['total_failed'] += len(news)

try:
if insights:
supabase_client.table("market_insights").upsert(insights, on_conflict="date").execute()
stats['market_insights_inserted'] = len(insights)
logger.info(f" ✓ market_insights: {len(insights)}")
except Exception as e:
logger.error(f" ✗ market_insights: {str(e)[:100]}")
stats['total_failed'] += len(insights)

return stats
```

# ═════════════════════════════════════════════════════════════════════════════════

# SECTION 6: ENDPOINTS (Health + ETL + On-demand)

# ═════════════════════════════════════════════════════════════════════════════════

@app.get(”/”)
def root():
return {
“name”: “SkillVault Silver Tier API”,
“version”: “2.0.0”,
“status”: “running”,
“tier”: “Silver” if VNSTOCK_TOKEN else “Free”,
“features”: [“VN30 daily”, “On-demand query”, “Smart cache”, “Parallel fetch”]
}

@app.get(”/api/health”)
def health():
“”“Health check with detailed connection status”””
supabase, vnstock, error = init_clients_lazy()
return {
“status”: “ok”,
“timestamp”: datetime.utcnow().isoformat(),
“supabase_connected”: supabase is not None,
“vnstock_connected”: vnstock is not None,
“vnstock_tier”: “Silver” if VNSTOCK_TOKEN else “Free”,
“error”: error,
“cache_size”: len(request_cache)
}

@app.post(”/api/etl/run”)
def run_etl_scheduled():
“”“Scheduled daily ETL for VN30 (Main pipeline)”””
logger.info(”[START] Scheduled ETL - VN30 Daily”)
logger.info(“═” * 80)

```
try:
supabase_client, vnstock_client, init_error = init_clients_lazy()

if init_error:
return {"status": "failed", "error": init_error, "timestamp": datetime.utcnow().isoformat()}

if not vnstock_client or not supabase_client:
return {"status": "failed", "error": "Clients not initialized", "timestamp": datetime.utcnow().isoformat()}

# ⭐ Use parallel fetching for VN30
market_data_list = fetch_market_data_parallel(vnstock_client, SYMBOLS_VN30)
fundamentals_list = fetch_fundamentals_parallel(vnstock_client, SYMBOLS_VN30)
macro_list = fetch_macro_data(vnstock_client)
news_list = fetch_news_data(vnstock_client, SYMBOLS_VN30)
insights_list = fetch_market_insights(vnstock_client)

# Calculate technicals
market_data_enriched = calculate_technicals(market_data_list)

# Validate
market_data_records, _ = validate_records(
dataframe_to_records(pd.concat(market_data_enriched)) if market_data_enriched else [],
"market_data", 80
)
fundamentals_records, _ = validate_records(fundamentals_list, "fundamentals", 70)
macro_records, _ = validate_records(macro_list, "macro_data", 75)
news_records, _ = validate_records(news_list, "news_intelligence", 60)
insights_records, _ = validate_records(insights_list, "market_insights", 80)

# Upsert
stats = upsert_to_supabase(supabase_client, market_data_records, fundamentals_records, macro_records, news_records, insights_records)

logger.info("[SUCCESS] Scheduled ETL complete")
logger.info("═" * 80)

return {
"status": "success",
"timestamp": datetime.utcnow().isoformat(),
"type": "scheduled_vn30_daily",
"stats": stats
}

except Exception as e:
logger.error(f"[FAILED] {str(e)}")
return {"status": "failed", "error": str(e), "timestamp": datetime.utcnow().isoformat()}
```

@app.get(”/api/stock/{symbol}”)
def fetch_single_symbol(symbol: str):
“”“⭐ ON-DEMAND: Fetch any symbol on-the-fly (flexible query)”””
logger.info(f”[ON-DEMAND] Fetching {symbol}…”)

```
try:
supabase_client, vnstock_client, init_error = init_clients_lazy()

if init_error:
raise HTTPException(status_code=503, detail=f"Service error: {init_error}")

if not vnstock_client:
raise HTTPException(status_code=503, detail="vnstock not initialized")

# Check cache first
cache_key = f"{symbol}_{datetime.now().strftime('%Y-%m-%d')}"
if cache_key in request_cache:
logger.info(f" {symbol}: Served from cache")
return request_cache[cache_key]

# Fetch fresh
logger.info(f" {symbol}: Fetching fresh data...")

market_df = fetch_market_data_single(vnstock_client, symbol)
if market_df is None:
raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found or no data available")

fundamentals = fetch_fundamentals_single(vnstock_client, symbol)
news = fetch_news_data(vnstock_client, [symbol])

# Calculate technicals
market_enriched = calculate_technicals([market_df])

# Build response
response = {
"symbol": symbol,
"timestamp": datetime.utcnow().isoformat(),
"market_data": {
"rows": len(market_df),
"latest_close": float(market_df.iloc[-1]['close']) if len(market_df) > 0 else None,
"ma20": float(market_df.iloc[-1].get('ma20', None)) if len(market_df) > 0 else None,
"ma50": float(market_df.iloc[-1].get('ma50', None)) if len(market_df) > 0 else None,
"rsi": float(market_df.iloc[-1].get('rsi', None)) if len(market_df) > 0 else None,
"foreign_net_flow": int(market_df.iloc[-1].get('foreign_net_flow', None)) if len(market_df) > 0 else None,
},
"fundamentals": fundamentals[:4] if fundamentals else [],
"news": news[:5] if news else [],
"cached": False
}

# Cache the response
request_cache[cache_key] = response
cache.set(f"{symbol}_market_data", market_df)

return response

except HTTPException as e:
raise e
except Exception as e:
logger.error(f" ✗ {symbol}: {str(e)}")
raise HTTPException(status_code=500, detail=f"Error fetching {symbol}: {str(e)}")
```

@app.get(”/api/cost”)
def show_cost_metrics():
“”“Monitor storage usage & cost (to stay within Free tier limits)”””
try:
supabase_client, _, _ = init_clients_lazy()

```
if not supabase_client:
return {"error": "Supabase not initialized"}

# Query row counts
market_count = supabase_client.table("market_data").select("count", count="exact").execute().count or 0
fundamentals_count = supabase_client.table("fundamentals").select("count", count="exact").execute().count or 0
news_count = supabase_client.table("news_intelligence").select("count", count="exact").execute().count or 0
macro_count = supabase_client.table("macro_data").select("count", count="exact").execute().count or 0

total_rows = market_count + fundamentals_count + news_count + macro_count

# Estimate size (rough)
estimated_size_mb = (total_rows * 0.005) # ~5KB per row average

return {
"status": "cost_metrics",
"timestamp": datetime.utcnow().isoformat(),
"table_counts": {
"market_data": market_count,
"fundamentals": fundamentals_count,
"news_intelligence": news_count,
"macro_data": macro_count,
"total_rows": total_rows
},
"estimated_size_mb": round(estimated_size_mb, 2),
"supabase_free_limit_mb": 500,
"usage_percentage": round((estimated_size_mb / 500) * 100, 2),
"cache": {
"size": len(request_cache),
"symbols_cached": len(cache.cache)
}
}

except Exception as e:
return {"error": str(e)}
```

# ═════════════════════════════════════════════════════════════════════════════════

# VERCEL HANDLER

# ═════════════════════════════════════════════════════════════════════════════════

handler = app
