#!/usr/bin/env python3
# ═══════════════════════════════════════════════════════════════════════════════
# SKILLVAULT PHASE 1 - Vercel Serverless (api/index.py) - WITH DEBUG LOGGING
# ═══════════════════════════════════════════════════════════════════════════════

import os
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

import pandas as pd
import numpy as np
from fastapi import FastAPI
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
VNSTOCK_TOKEN = os.getenv("VNSTOCK_TOKEN", "")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

SYMBOLS = ["TCB", "VHM", "BID"]
FETCH_PERIOD = "1y"
CONFIDENCE_THRESHOLDS = {
    "market_data": 80,
    "fundamentals": 70,
    "macro_data": 75,
    "news_intelligence": 60,
    "market_insights": 80
}

logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="SkillVault ETL API", version="1.0.0")

supabase_client = None
vnstock_client = None
clients_initialized = False
init_error = None


# ═══════════════════════════════════════════════════════════════════════════════
# LAZY INITIALIZATION
# ═══════════════════════════════════════════════════════════════════════════════

def init_clients_lazy():
    global supabase_client, vnstock_client, clients_initialized, init_error
    
    if clients_initialized:
        return supabase_client, vnstock_client, init_error
    
    clients_initialized = True
    
    try:
        if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
            init_error = "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY"
            logger.error(f"[INIT] {init_error}")
            return None, None, init_error
        
        try:
            from supabase import create_client
            supabase_client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
            logger.info("[INIT] Supabase initialized ✓")
        except Exception as e:
            init_error = f"Supabase init failed: {str(e)}"
            logger.error(f"[INIT] {init_error}")
            return None, None, init_error
        
        try:
            from vnstock import Vnstock
            vnstock_client = Vnstock()
            logger.info("[INIT] vnstock initialized ✓")
        except Exception as e:
            init_error = f"vnstock init failed: {str(e)}"
            logger.error(f"[INIT] {init_error}")
            return supabase_client, None, init_error
        
        return supabase_client, vnstock_client, None
    
    except Exception as e:
        init_error = f"Unexpected init error: {str(e)}"
        logger.error(f"[INIT] {init_error}")
        return None, None, init_error


# ═══════════════════════════════════════════════════════════════════════════════
# FETCH FUNCTIONS (WITH DEBUG LOGGING)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_market_data(vnstock_client) -> List[pd.DataFrame]:
    logger.info("[FETCH] market_data...")
    market_data_list = []
    
    for symbol in SYMBOLS:
        try:
            logger.info(f"  Fetching {symbol}...")
            df = vnstock_client.stock(symbol=symbol).history(period=FETCH_PERIOD)
            logger.info(f"  {symbol}: raw df shape = {df.shape}")
            
            if len(df) == 0:
                logger.warning(f"  {symbol}: df is EMPTY!")
                continue
            
            df['symbol'] = symbol
            df['source'] = 'vnstock'
            df['fetch_timestamp'] = datetime.utcnow().isoformat()
            df['confidence_score'] = 100
            market_data_list.append(df)
            logger.info(f"  ✓ {symbol}: {len(df)} records added")
        except Exception as e:
            logger.error(f"  ✗ {symbol}: {str(e)}")
    
    logger.info(f"[FETCH] market_data total: {len(market_data_list)} DataFrames")
    return market_data_list


def fetch_fundamentals(vnstock_client) -> List[Dict]:
    logger.info("[FETCH] fundamentals...")
    fundamentals_list = []
    
    for symbol in SYMBOLS:
        try:
            logger.info(f"  Fetching {symbol}...")
            ratio_df = vnstock_client.stock(symbol=symbol).ratio()
            income_df = vnstock_client.stock(symbol=symbol).income_statement()
            
            logger.info(f"  {symbol}: ratio_df shape = {ratio_df.shape}, income_df shape = {income_df.shape}")
            
            for idx, row in income_df.iterrows():
                try:
                    publish_date = pd.to_datetime(row.get('date', datetime.now()))
                    record = {
                        'symbol': symbol,
                        'fiscal_year': publish_date.year,
                        'fiscal_quarter': (publish_date.month - 1) // 3 + 1,
                        'pe_ratio': float(ratio_df.iloc[0].get('pe', None)) if len(ratio_df) > 0 else None,
                        'pb_ratio': float(ratio_df.iloc[0].get('pb', None)) if len(ratio_df) > 0 else None,
                        'roe': float(ratio_df.iloc[0].get('roe', None)) if len(ratio_df) > 0 else None,
                        'roa': float(ratio_df.iloc[0].get('roa', None)) if len(ratio_df) > 0 else None,
                        'revenue_ttm': int(row.get('revenue', 0)) if row.get('revenue') else None,
                        'profit_ttm': int(row.get('profit', 0)) if row.get('profit') else None,
                        'eps': float(row.get('eps', None)) if row.get('eps') else None,
                        'dividend_yield': float(ratio_df.iloc[0].get('dividend_yield', None)) if len(ratio_df) > 0 else None,
                        'publish_date': publish_date.strftime('%Y-%m-%d'),
                        'source': 'vnstock',
                        'fetch_timestamp': datetime.utcnow().isoformat(),
                        'confidence_score': 100
                    }
                    fundamentals_list.append(record)
                except Exception as e:
                    logger.warning(f"    skip row: {str(e)[:50]}")
        except Exception as e:
            logger.error(f"  ✗ {symbol}: {str(e)}")
    
    logger.info(f"[FETCH] fundamentals total: {len(fundamentals_list)} records")
    return fundamentals_list


def fetch_macro_data(vnstock_client) -> List[Dict]:
    logger.info("[FETCH] macro_data...")
    try:
        macro_df = vnstock_client.macro()
        logger.info(f"  macro_df shape = {macro_df.shape}")
        
        macro_list = []
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
                    'pmi_manufacturing': float(row.get('pmi', None)) if row.get('pmi') else None,
                    'source': 'vnstock',
                    'fetch_timestamp': datetime.utcnow().isoformat(),
                    'confidence_score': 100
                }
                macro_list.append(record)
            except Exception as e:
                logger.warning(f"  skip macro row: {str(e)[:50]}")
        
        logger.info(f"[FETCH] macro_data total: {len(macro_list)} records")
        return macro_list
    except Exception as e:
        logger.error(f"[FETCH] macro failed: {str(e)}")
        return []


def fetch_news_data(vnstock_client) -> List[Dict]:
    logger.info("[FETCH] news_intelligence...")
    news_list = []
    
    for symbol in SYMBOLS:
        try:
            logger.info(f"  Fetching news for {symbol}...")
            news_df = vnstock_client.stock(symbol=symbol).news(limit=50)
            logger.info(f"  {symbol}: news_df shape = {news_df.shape}")
            
            for idx, row in news_df.iterrows():
                try:
                    record = {
                        'symbol': symbol,
                        'date': pd.to_datetime(row.get('publish_date', datetime.now())).strftime('%Y-%m-%d'),
                        'title': str(row.get('title', ''))[:500],
                        'summary': str(row.get('description', '')) if row.get('description') else None,
                        'source': str(row.get('source', 'unknown')),
                        'url': str(row.get('url', None)) if row.get('url') else None,
                        'sentiment': None,
                        'sentiment_confidence': None,
                        'key_facts': None,
                        'fetch_timestamp': datetime.utcnow().isoformat(),
                        'confidence_score': 80
                    }
                    news_list.append(record)
                except Exception as e:
                    logger.warning(f"    skip news row: {str(e)[:50]}")
        except Exception as e:
            logger.error(f"  ✗ {symbol} news: {str(e)}")
    
    logger.info(f"[FETCH] news_intelligence total: {len(news_list)} records")
    return news_list


def fetch_market_insights(vnstock_client) -> List[Dict]:
    logger.info("[FETCH] market_insights...")
    try:
        vnindex_df = vnstock_client.stock(symbol='VNINDEX').quote()
        logger.info(f"  vnindex_df shape = {vnindex_df.shape}")
        
        record = {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'vnindex_close': float(vnindex_df.iloc[0].get('price', None)) if len(vnindex_df) > 0 else None,
            'vnindex_change': float(vnindex_df.iloc[0].get('change', None)) if len(vnindex_df) > 0 else None,
            'vnindex_change_percent': float(vnindex_df.iloc[0].get('change_percent', None)) if len(vnindex_df) > 0 else None,
            'top_gainer': None,
            'top_gainer_change': None,
            'top_loser': None,
            'top_loser_change': None,
            'breadth_advance': None,
            'breadth_decline': None,
            'breadth_unchanged': None,
            'foreign_buy_net': None,
            'total_liquidity': None,
            'vnindex_pe': None,
            'vnindex_pb': None,
            'source': 'vnstock',
            'fetch_timestamp': datetime.utcnow().isoformat(),
            'confidence_score': 80
        }
        logger.info(f"[FETCH] market_insights: {record['vnindex_close']}")
        return [record]
    except Exception as e:
        logger.error(f"[FETCH] market_insights failed: {str(e)}")
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# CALCULATE TECHNICALS
# ═══════════════════════════════════════════════════════════════════════════════

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
    logger.info("[CALC] technicals...")
    enriched_list = []
    
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
            logger.info(f"  ✓ calculated technicals for {df['symbol'].iloc[0]}")
        except Exception as e:
            logger.error(f"  ✗ calc error: {str(e)}")
            enriched_list.append(df)
    
    logger.info(f"[CALC] enriched {len(enriched_list)} DataFrames")
    return enriched_list


# ═══════════════════════════════════════════════════════════════════════════════
# VALIDATE & UPSERT
# ═══════════════════════════════════════════════════════════════════════════════

def validate_records(records: List[Dict], table_name: str, threshold: int) -> Tuple[List[Dict], int]:
    logger.info(f"[VALIDATE] {table_name} ({len(records)} records)...")
    valid_records = []
    skipped = 0
    
    for record in records:
        try:
            non_null = sum(1 for v in record.values() if v is not None)
            completeness = (non_null / len(record)) * 100
            
            if completeness >= 90:
                confidence = 100
            elif completeness >= 70:
                confidence = 80
            else:
                confidence = 60
            
            record['confidence_score'] = min(record.get('confidence_score', 100), confidence)
            
            if record.get('confidence_score', 0) >= threshold:
                valid_records.append(record)
            else:
                skipped += 1
                logger.debug(f"  skipped (confidence={record.get('confidence_score')})")
        except Exception as e:
            skipped += 1
            logger.warning(f"  skip record: {str(e)[:50]}")
    
    logger.info(f"  Valid: {len(valid_records)}, Skipped: {skipped}")
    return valid_records, skipped


def dataframe_to_records(df: pd.DataFrame) -> List[Dict]:
    records = []
    for idx, row in df.iterrows():
        record = row.dropna().to_dict()
        for key in ['date', 'publish_date']:
            if key in record and isinstance(record[key], pd.Timestamp):
                record[key] = record[key].strftime('%Y-%m-%d')
        for key, value in record.items():
            if isinstance(value, (np.integer, np.floating)):
                record[key] = float(value) if isinstance(value, np.floating) else int(value)
        records.append(record)
    return records


def upsert_to_supabase(supabase_client, market_data, fundamentals, macro, news, insights) -> Dict:
    logger.info("[UPSERT]...")
    stats = {
        'market_data_inserted': 0,
        'fundamentals_inserted': 0,
        'macro_inserted': 0,
        'news_inserted': 0,
        'market_insights_inserted': 0,
        'total_failed': 0
    }
    
    if not supabase_client:
        logger.error("[UPSERT] No supabase_client!")
        return stats
    
    if market_data:
        try:
            supabase_client.table("market_data").upsert(market_data, on_conflict="symbol,date").execute()
            stats['market_data_inserted'] = len(market_data)
            logger.info(f"  ✓ market_data: {len(market_data)}")
        except Exception as e:
            logger.error(f"  ✗ market_data: {str(e)[:100]}")
            stats['total_failed'] += len(market_data)
    else:
        logger.warning("[UPSERT] No market_data to insert!")
    
    if fundamentals:
        try:
            supabase_client.table("fundamentals").upsert(fundamentals, on_conflict="symbol,fiscal_year,fiscal_quarter").execute()
            stats['fundamentals_inserted'] = len(fundamentals)
            logger.info(f"  ✓ fundamentals: {len(fundamentals)}")
        except Exception as e:
            logger.error(f"  ✗ fundamentals: {str(e)[:100]}")
            stats['total_failed'] += len(fundamentals)
    else:
        logger.warning("[UPSERT] No fundamentals to insert!")
    
    if macro:
        try:
            supabase_client.table("macro_data").upsert(macro, on_conflict="date,period_type").execute()
            stats['macro_inserted'] = len(macro)
            logger.info(f"  ✓ macro_data: {len(macro)}")
        except Exception as e:
            logger.error(f"  ✗ macro_data: {str(e)[:100]}")
            stats['total_failed'] += len(macro)
    else:
        logger.warning("[UPSERT] No macro_data to insert!")
    
    if news:
        try:
            supabase_client.table("news_intelligence").insert(news).execute()
            stats['news_inserted'] = len(news)
            logger.info(f"  ✓ news_intelligence: {len(news)}")
        except Exception as e:
            logger.error(f"  ✗ news_intelligence: {str(e)[:100]}")
            stats['total_failed'] += len(news)
    else:
        logger.warning("[UPSERT] No news_intelligence to insert!")
    
    if insights:
        try:
            supabase_client.table("market_insights").upsert(insights, on_conflict="date").execute()
            stats['market_insights_inserted'] = len(insights)
            logger.info(f"  ✓ market_insights: {len(insights)}")
        except Exception as e:
            logger.error(f"  ✗ market_insights: {str(e)[:100]}")
            stats['total_failed'] += len(insights)
    else:
        logger.warning("[UPSERT] No market_insights to insert!")
    
    return stats


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/")
def root():
    return {
        "name": "SkillVault ETL API",
        "version": "1.0.0",
        "status": "running",
        "symbols_testing": SYMBOLS
    }


@app.get("/api/health")
def health():
    supabase, vnstock, error = init_clients_lazy()
    return {
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat(),
        "supabase_connected": supabase is not None,
        "vnstock_connected": vnstock is not None,
        "error": error
    }


@app.post("/api/etl/run")
def run_etl():
    logger.info("[START] ETL Pipeline")
    logger.info("═" * 80)
    
    try:
        supabase_client, vnstock_client, init_error = init_clients_lazy()
        
        if init_error:
            logger.error(f"[FAILED] {init_error}")
            return {
                "status": "failed",
                "error": init_error,
                "timestamp": datetime.utcnow().isoformat()
            }
        
        if not vnstock_client or not supabase_client:
            logger.error("[FAILED] Clients not initialized")
            return {
                "status": "failed",
                "error": "Clients not initialized",
                "timestamp": datetime.utcnow().isoformat()
            }
        
        # FETCH
        logger.info("\n[STAGE 2] FETCH")
        market_data_list = fetch_market_data(vnstock_client)
        fundamentals_list = fetch_fundamentals(vnstock_client)
        macro_list = fetch_macro_data(vnstock_client)
        news_list = fetch_news_data(vnstock_client)
        insights_list = fetch_market_insights(vnstock_client)
        
        # CALCULATE
        logger.info("\n[STAGE 3] CALCULATE")
        market_data_enriched = calculate_technicals(market_data_list)
        
        # VALIDATE
        logger.info("\n[STAGE 4] VALIDATE")
        market_data_records, _ = validate_records(
            dataframe_to_records(pd.concat(market_data_enriched)) if market_data_enriched else [],
            "market_data", 80
        )
        fundamentals_records, _ = validate_records(fundamentals_list, "fundamentals", 70)
        macro_records, _ = validate_records(macro_list, "macro_data", 75)
        news_records, _ = validate_records(news_list, "news_intelligence", 60)
        insights_records, _ = validate_records(insights_list, "market_insights", 80)
        
        # UPSERT
        logger.info("\n[STAGE 5] UPSERT")
        stats = upsert_to_supabase(supabase_client, market_data_records, fundamentals_records, macro_records, news_records, insights_records)
        
        logger.info("\n" + "═" * 80)
        logger.info("[SUCCESS] ETL Pipeline complete")
        logger.info("═" * 80)
        
        return {
            "status": "success",
            "timestamp": datetime.utcnow().isoformat(),
            "stats": stats,
            "symbols_processed": SYMBOLS
        }
    
    except Exception as e:
        logger.error(f"\n[FAILED] {str(e)}")
        logger.error("═" * 80)
        return {
            "status": "failed",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }


# ═══════════════════════════════════════════════════════════════════════════════
# VERCEL HANDLER
# ═══════════════════════════════════════════════════════════════════════════════

handler = app
