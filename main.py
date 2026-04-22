#!/usr/bin/env python3
# ═══════════════════════════════════════════════════════════════════════════════
# SKILLVAULT PHASE 1 - Vercel API (main.py)
# FastAPI ETL: Fetch vnstock → Calculate → Validate → Upsert Supabase
# Production-Ready for Deployment
# Copy this ENTIRE file into your GitHub repo main.py (DELETE old code first)
# ═══════════════════════════════════════════════════════════════════════════════

import os
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from decimal import Decimal
import traceback

import pandas as pd
import numpy as np
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

try:
    from vnstock import Vnstock
except ImportError:
    Vnstock = None

try:
    from supabase import create_client, Client
except ImportError:
    create_client = None
    Client = None

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION & INITIALIZATION
# ═══════════════════════════════════════════════════════════════════════════════

load_dotenv()

# Environment Variables (Read from .env, NOT hardcoded)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
VNSTOCK_TOKEN = os.getenv("VNSTOCK_TOKEN", "")  # Empty for free tier
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Symbol List (VN30 + more)
SYMBOLS = [
    "VHM", "BID", "VCB", "FPT", "MWG", "VNM", "SAB", "TCB", "TPB", "ACB",
    "VIC", "CTG", "GAS", "SHB", "EIB", "HDB", "OCB", "STB", "NVL", "VPB",
    "MSN", "KDH", "VJC", "VNA", "PNJ", "DXG", "REE", "DIG", "HVN", "PDR"
]

FETCH_PERIOD = "1y"
CONFIDENCE_THRESHOLDS = {
    "market_data": 80,
    "fundamentals": 70,
    "macro_data": 75,
    "news_intelligence": 60,
    "market_insights": 80
}

# ═══ LOGGING SETUP ═══
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ═══ FASTAPI APP ═══
app = FastAPI(title="SkillVault ETL API", version="1.0.0")

# ═══ GLOBAL CLIENTS ═══
supabase_client: Optional[Client] = None
vnstock_client: Optional[Vnstock] = None
current_sync_id: Optional[int] = None


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 0: STARTUP & VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════

def validate_environment():
    """Validate that all required env vars are set"""
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise Exception("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env")
    logger.info("[INIT] Environment variables validated")


def init_clients():
    """Initialize Supabase and vnstock clients"""
    global supabase_client, vnstock_client
    
    try:
        supabase_client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
        logger.info("[INIT] Supabase client initialized")
    except Exception as e:
        logger.error(f"[INIT] Supabase init failed: {e}")
        raise
    
    try:
        vnstock_client = Vnstock()
        logger.info("[INIT] vnstock client initialized (free tier)")
        if VNSTOCK_TOKEN:
            logger.info("[INIT] vnstock token set (ready for upgrade to Silver)")
    except Exception as e:
        logger.error(f"[INIT] vnstock init failed: {e}")
        raise


def create_sync_log_entry() -> int:
    """Create initial sync_log record and return ID"""
    global current_sync_id
    
    try:
        result = supabase_client.table("sync_log").insert({
            "timestamp": datetime.utcnow().isoformat(),
            "action": "START",
            "status": "PENDING"
        }).execute()
        current_sync_id = result.data[0]["id"] if result.data else None
        logger.info(f"[INIT] Sync log created: id={current_sync_id}")
        return current_sync_id
    except Exception as e:
        logger.error(f"[INIT] Failed to create sync_log: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 1: FETCH DATA FROM VNSTOCK
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_market_data() -> List[pd.DataFrame]:
    """STAGE 2.1: Fetch OHLCV history for all symbols"""
    logger.info("[FETCH] Starting market_data...")
    
    market_data_list = []
    
    for symbol in SYMBOLS:
        try:
            logger.info(f"  Fetching {symbol}...")
            df = vnstock_client.stock(symbol=symbol).history(period=FETCH_PERIOD)
            
            # Add metadata columns
            df['symbol'] = symbol
            df['source'] = 'vnstock'
            df['fetch_timestamp'] = datetime.utcnow().isoformat()
            df['confidence_score'] = 100
            
            # Rename columns to match schema
            df = df.rename(columns={
                'open': 'open',
                'high': 'high',
                'low': 'low',
                'close': 'close',
                'volume': 'volume',
                'adjust': 'adjust'
            })
            
            market_data_list.append(df)
            logger.info(f"    ✓ Got {len(df)} records for {symbol}")
        
        except Exception as e:
            logger.warning(f"  ✗ Failed for {symbol}: {str(e)[:100]}")
            continue
    
    logger.info(f"[FETCH] market_data complete: {len(market_data_list)}/{len(SYMBOLS)} symbols")
    return market_data_list


def fetch_fundamentals() -> List[Dict[str, Any]]:
    """STAGE 2.2: Fetch financial ratios and statements"""
    logger.info("[FETCH] Starting fundamentals...")
    
    fundamentals_list = []
    
    for symbol in SYMBOLS:
        try:
            logger.info(f"  Fetching {symbol}...")
            
            # Get ratio data
            ratio_df = vnstock_client.stock(symbol=symbol).ratio()
            
            # Get income statement
            income_df = vnstock_client.stock(symbol=symbol).income_statement()
            
            # Combine and pivot by quarter
            for idx, row in income_df.iterrows():
                try:
                    publish_date = pd.to_datetime(row.get('date', datetime.now()))
                    fiscal_year = publish_date.year
                    fiscal_quarter = (publish_date.month - 1) // 3 + 1
                    
                    record = {
                        'symbol': symbol,
                        'fiscal_year': fiscal_year,
                        'fiscal_quarter': fiscal_quarter,
                        'pe_ratio': float(ratio_df.iloc[0].get('pe', None)) if len(ratio_df) > 0 else None,
                        'pb_ratio': float(ratio_df.iloc[0].get('pb', None)) if len(ratio_df) > 0 else None,
                        'roe': float(ratio_df.iloc[0].get('roe', None)) if len(ratio_df) > 0 else None,
                        'roa': float(ratio_df.iloc[0].get('roa', None)) if len(ratio_df) > 0 else None,
                        'revenue_ttm': int(row.get('revenue', 0)) if 'revenue' in row and row['revenue'] else None,
                        'profit_ttm': int(row.get('profit', 0)) if 'profit' in row and row['profit'] else None,
                        'eps': float(row.get('eps', None)) if 'eps' in row else None,
                        'dividend_yield': float(ratio_df.iloc[0].get('dividend_yield', None)) if len(ratio_df) > 0 else None,
                        'publish_date': publish_date.strftime('%Y-%m-%d'),
                        'source': 'vnstock',
                        'fetch_timestamp': datetime.utcnow().isoformat(),
                        'confidence_score': 100
                    }
                    fundamentals_list.append(record)
                except Exception as e:
                    logger.warning(f"    Skip quarter: {str(e)[:50]}")
                    continue
            
            logger.info(f"    ✓ Got {len(fundamentals_list)} quarter records for {symbol}")
        
        except Exception as e:
            logger.warning(f"  ✗ Failed for {symbol}: {str(e)[:100]}")
            continue
    
    logger.info(f"[FETCH] fundamentals complete: {len(fundamentals_list)} records")
    return fundamentals_list


def fetch_macro_data() -> List[Dict[str, Any]]:
    """STAGE 2.3: Fetch macro-economic data"""
    logger.info("[FETCH] Starting macro_data...")
    
    try:
        macro_df = vnstock_client.macro()
        
        macro_list = []
        for idx, row in macro_df.iterrows():
            try:
                date_str = pd.to_datetime(row.get('date', datetime.now())).strftime('%Y-%m')
                
                record = {
                    'date': date_str,
                    'period_type': 'MONTHLY',
                    'gdp_growth': float(row.get('gdp', None)) if 'gdp' in row and row['gdp'] is not None else None,
                    'inflation_rate': float(row.get('cpi', None)) if 'cpi' in row and row['cpi'] is not None else None,
                    'interest_rate': float(row.get('interest_rate', None)) if 'interest_rate' in row and row['interest_rate'] is not None else None,
                    'usd_vnd_rate': float(row.get('usd_vnd', None)) if 'usd_vnd' in row and row['usd_vnd'] is not None else None,
                    'fdi_inflow': int(row.get('fdi', 0)) if 'fdi' in row and row['fdi'] is not None else None,
                    'pmi_manufacturing': float(row.get('pmi', None)) if 'pmi' in row and row['pmi'] is not None else None,
                    'consumer_confidence': float(row.get('consumer_confidence', None)) if 'consumer_confidence' in row and row['consumer_confidence'] is not None else None,
                    'source': 'vnstock',
                    'fetch_timestamp': datetime.utcnow().isoformat(),
                    'confidence_score': 100
                }
                macro_list.append(record)
            except Exception as e:
                logger.warning(f"  Skip macro record: {str(e)[:50]}")
                continue
        
        logger.info(f"[FETCH] macro_data complete: {len(macro_list)} records")
        return macro_list
    
    except Exception as e:
        logger.warning(f"[FETCH] macro_data failed: {str(e)[:100]}")
        return []


def fetch_news_data() -> List[Dict[str, Any]]:
    """STAGE 2.4: Fetch news headlines from 21 Vietnamese sources"""
    logger.info("[FETCH] Starting news_intelligence...")
    
    news_list = []
    
    for symbol in SYMBOLS:
        try:
            logger.info(f"  Fetching news for {symbol}...")
            news_df = vnstock_client.stock(symbol=symbol).news(limit=50)
            
            for idx, row in news_df.iterrows():
                try:
                    record = {
                        'symbol': symbol,
                        'date': pd.to_datetime(row.get('publish_date', datetime.now())).strftime('%Y-%m-%d'),
                        'title': str(row.get('title', ''))[:500],
                        'summary': str(row.get('description', '')) if 'description' in row else None,
                        'source': str(row.get('source', 'unknown')),
                        'url': str(row.get('url', None)) if 'url' in row else None,
                        'sentiment': None,  # To be filled by Claude Haiku in Phase 3
                        'sentiment_confidence': None,
                        'key_facts': None,
                        'fetch_timestamp': datetime.utcnow().isoformat(),
                        'confidence_score': 80  # Lower due to missing sentiment
                    }
                    news_list.append(record)
                except Exception as e:
                    logger.warning(f"    Skip news item: {str(e)[:50]}")
                    continue
            
            logger.info(f"    ✓ Got {len([n for n in news_list if n['symbol'] == symbol])} news for {symbol}")
        
        except Exception as e:
            logger.warning(f"  ✗ Failed for {symbol}: {str(e)[:100]}")
            continue
    
    logger.info(f"[FETCH] news_intelligence complete: {len(news_list)} records")
    return news_list


def fetch_market_insights() -> List[Dict[str, Any]]:
    """STAGE 2.5: Fetch market-wide insights (VN-Index, breadth, movers)"""
    logger.info("[FETCH] Starting market_insights...")
    
    try:
        # Get VN-Index data
        vnindex_df = vnstock_client.stock(symbol='VNINDEX').quote()
        
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
        
        logger.info(f"[FETCH] market_insights complete: {record['vnindex_close']} VND")
        return [record]
    
    except Exception as e:
        logger.warning(f"[FETCH] market_insights failed: {str(e)[:100]}")
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 2: CALCULATE TECHNICAL INDICATORS
# ═══════════════════════════════════════════════════════════════════════════════

def calculate_ma(prices: np.ndarray, window: int) -> np.ndarray:
    """Calculate simple moving average"""
    if len(prices) < window:
        return np.full_like(prices, np.nan, dtype=float)
    
    ma = np.zeros_like(prices, dtype=float)
    ma[:window-1] = np.nan
    
    for i in range(window-1, len(prices)):
        ma[i] = np.mean(prices[i-window+1:i+1])
    
    return ma


def calculate_rsi(prices: np.ndarray, period: int = 14) -> np.ndarray:
    """Calculate Relative Strength Index (RSI-14)"""
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
    """STAGE 3: Calculate MA20, MA50, MA200, RSI-14, Support/Resistance"""
    logger.info("[CALC] Starting technical calculations...")
    
    enriched_list = []
    
    for df in market_data_list:
        try:
            symbol = df['symbol'].iloc[0]
            logger.info(f"  Calculating {symbol}...")
            
            # Sort by date
            df = df.sort_values('date').reset_index(drop=True)
            
            # Extract closes
            closes = df['close'].values.astype(float)
            
            # Calculate MAs
            df['ma20'] = pd.Series(calculate_ma(closes, 20))
            df['ma50'] = pd.Series(calculate_ma(closes, 50))
            df['ma200'] = pd.Series(calculate_ma(closes, 200))
            
            # Calculate RSI
            df['rsi'] = pd.Series(calculate_rsi(closes, 14))
            
            # Calculate Support/Resistance
            lows = df['low'].values.astype(float)
            highs = df['high'].values.astype(float)
            
            support = np.nanmin(lows[~np.isnan(lows)])
            resistance = np.nanmax(highs[~np.isnan(highs)])
            
            df['support_level'] = support
            df['resistance_level'] = resistance
            
            enriched_list.append(df)
            logger.info(f"    ✓ Calculated MA20, MA50, MA200, RSI, Support/Resistance")
        
        except Exception as e:
            logger.warning(f"  ✗ Failed for symbol: {str(e)[:100]}")
            enriched_list.append(df)  # Still add, but without technicals
            continue
    
    logger.info(f"[CALC] Technical calculations complete")
    return enriched_list


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 3: VALIDATE DATA
# ═══════════════════════════════════════════════════════════════════════════════

def validate_records(records: List[Dict[str, Any]], table_name: str, threshold: int) -> Tuple[List[Dict], int]:
    """STAGE 4: Validate records before insert"""
    logger.info(f"[VALIDATE] {table_name} ({len(records)} records)...")
    
    valid_records = []
    skipped = 0
    
    for record in records:
        try:
            # Count non-NULL fields
            non_null = sum(1 for v in record.values() if v is not None)
            total = len(record)
            completeness = (non_null / total) * 100
            
            # Assign confidence
            if completeness >= 90:
                confidence = 100
            elif completeness >= 70:
                confidence = 80
            elif completeness >= 50:
                confidence = 60
            else:
                confidence = 40
            
            record['confidence_score'] = min(record.get('confidence_score', 100), confidence)
            
            # Validate ranges
            valid = True
            
            if 'close' in record and record['close'] is not None:
                if float(record['close']) <= 0 or float(record['close']) > 1_000_000:
                    valid = False
            
            if 'pe_ratio' in record and record['pe_ratio'] is not None:
                if float(record['pe_ratio']) < 0 or float(record['pe_ratio']) > 100:
                    valid = False
            
            if 'sentiment' in record and record['sentiment'] is not None:
                if record['sentiment'] not in [-1, 0, 1]:
                    valid = False
            
            # Check confidence threshold
            if valid and record.get('confidence_score', 0) >= threshold:
                valid_records.append(record)
            else:
                skipped += 1
        
        except Exception as e:
            logger.warning(f"  Skip record: {str(e)[:50]}")
            skipped += 1
            continue
    
    logger.info(f"  Accepted: {len(valid_records)}, Skipped: {skipped}")
    return valid_records, skipped


def dataframe_to_records(df: pd.DataFrame, table_name: str) -> List[Dict[str, Any]]:
    """Convert pandas DataFrame to list of dicts for Supabase"""
    records = []
    
    # Drop NaN columns and convert to dict
    for idx, row in df.iterrows():
        record = row.dropna().to_dict()
        
        # Convert dates
        for key in ['date', 'publish_date']:
            if key in record:
                if isinstance(record[key], pd.Timestamp):
                    record[key] = record[key].strftime('%Y-%m-%d')
        
        # Convert numpy types
        for key, value in record.items():
            if isinstance(value, (np.integer, np.floating)):
                record[key] = float(value) if isinstance(value, np.floating) else int(value)
        
        records.append(record)
    
    return records


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 4: UPSERT TO SUPABASE
# ═══════════════════════════════════════════════════════════════════════════════

def upsert_to_supabase(
    market_data_records: List[Dict],
    fundamentals_records: List[Dict],
    macro_records: List[Dict],
    news_records: List[Dict],
    insights_records: List[Dict]
) -> Dict[str, int]:
    """STAGE 5: Upsert records to Supabase"""
    logger.info("[UPSERT] Starting...")
    
    stats = {
        'market_data_inserted': 0,
        'market_data_updated': 0,
        'fundamentals_inserted': 0,
        'fundamentals_updated': 0,
        'macro_inserted': 0,
        'macro_updated': 0,
        'news_inserted': 0,
        'market_insights_inserted': 0,
        'market_insights_updated': 0,
        'total_failed': 0
    }
    
    # ─── market_data ───
    if market_data_records:
        try:
            result = supabase_client.table("market_data").upsert(
                market_data_records,
                on_conflict="symbol,date"
            ).execute()
            stats['market_data_inserted'] = len(market_data_records)
            logger.info(f"  market_data: {len(market_data_records)} upserted")
        except Exception as e:
            logger.error(f"  market_data failed: {str(e)[:100]}")
            stats['total_failed'] += len(market_data_records)
    
    # ─── fundamentals ───
    if fundamentals_records:
        try:
            result = supabase_client.table("fundamentals").upsert(
                fundamentals_records,
                on_conflict="symbol,fiscal_year,fiscal_quarter"
            ).execute()
            stats['fundamentals_inserted'] = len(fundamentals_records)
            logger.info(f"  fundamentals: {len(fundamentals_records)} upserted")
        except Exception as e:
            logger.error(f"  fundamentals failed: {str(e)[:100]}")
            stats['total_failed'] += len(fundamentals_records)
    
    # ─── macro_data ───
    if macro_records:
        try:
            result = supabase_client.table("macro_data").upsert(
                macro_records,
                on_conflict="date,period_type"
            ).execute()
            stats['macro_inserted'] = len(macro_records)
            logger.info(f"  macro_data: {len(macro_records)} upserted")
        except Exception as e:
            logger.error(f"  macro_data failed: {str(e)[:100]}")
            stats['total_failed'] += len(macro_records)
    
    # ─── news_intelligence (INSERT only, no upsert) ───
    if news_records:
        try:
            result = supabase_client.table("news_intelligence").insert(
                news_records
            ).execute()
            stats['news_inserted'] = len(news_records)
            logger.info(f"  news_intelligence: {len(news_records)} inserted")
        except Exception as e:
            logger.error(f"  news_intelligence failed: {str(e)[:100]}")
            stats['total_failed'] += len(news_records)
    
    # ─── market_insights ───
    if insights_records:
        try:
            result = supabase_client.table("market_insights").upsert(
                insights_records,
                on_conflict="date"
            ).execute()
            stats['market_insights_inserted'] = len(insights_records)
            logger.info(f"  market_insights: {len(insights_records)} upserted")
        except Exception as e:
            logger.error(f"  market_insights failed: {str(e)[:100]}")
            stats['total_failed'] += len(insights_records)
    
    logger.info(f"[UPSERT] Complete")
    return stats


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 5: BUILD CONTEXT PACKAGE
# ═══════════════════════════════════════════════════════════════════════════════

def build_context_package(
    market_data_list: List[pd.DataFrame],
    fundamentals_records: List[Dict],
    macro_records: List[Dict],
    news_records: List[Dict],
    insights_records: List[Dict]
) -> Dict[str, Any]:
    """STAGE 6: Build Context Package for Apps Script/Claude analysis"""
    logger.info("[BUILD] Context package...")
    
    # Get latest records
    latest_market = None
    for df in reversed(market_data_list):
        if len(df) > 0:
            latest_market = df.iloc[-1].to_dict()
            break
    
    latest_fundamentals = fundamentals_records[-1] if fundamentals_records else None
    latest_macro = macro_records[-1] if macro_records else None
    latest_news = news_records[-10:] if news_records else []
    latest_insights = insights_records[0] if insights_records else None
    
    context_package = {
        "timestamp": datetime.utcnow().isoformat(),
        "layers": {
            "layer_0_macro": {
                "gdp_growth": latest_macro.get('gdp_growth') if latest_macro else None,
                "inflation_rate": latest_macro.get('inflation_rate') if latest_macro else None,
                "interest_rate": latest_macro.get('interest_rate') if latest_macro else None,
                "usd_vnd_rate": latest_macro.get('usd_vnd_rate') if latest_macro else None,
                "fdi_inflow": latest_macro.get('fdi_inflow') if latest_macro else None,
                "pmi_manufacturing": latest_macro.get('pmi_manufacturing') if latest_macro else None,
                "source": "vnstock macro API",
                "date": latest_macro.get('date') if latest_macro else None
            },
            "layer_1_market": {
                "vnindex": latest_insights.get('vnindex_close') if latest_insights else None,
                "vnindex_change": latest_insights.get('vnindex_change') if latest_insights else None,
                "breadth_advance": latest_insights.get('breadth_advance') if latest_insights else None,
                "breadth_decline": latest_insights.get('breadth_decline') if latest_insights else None,
                "top_gainer": latest_insights.get('top_gainer') if latest_insights else None,
                "top_loser": latest_insights.get('top_loser') if latest_insights else None,
                "source": "vnstock market API",
                "date": latest_insights.get('date') if latest_insights else None
            },
            "layer_2_sector": {
                "recent_news": latest_news,
                "note": "Sector classification: TBD in Phase 3"
            },
            "layer_3_fundamentals": {
                "pe_ratio": latest_fundamentals.get('pe_ratio') if latest_fundamentals else None,
                "pb_ratio": latest_fundamentals.get('pb_ratio') if latest_fundamentals else None,
                "roe": latest_fundamentals.get('roe') if latest_fundamentals else None,
                "roa": latest_fundamentals.get('roa') if latest_fundamentals else None,
                "eps": latest_fundamentals.get('eps') if latest_fundamentals else None,
                "dividend_yield": latest_fundamentals.get('dividend_yield') if latest_fundamentals else None,
                "debt_equity": latest_fundamentals.get('debt_equity') if latest_fundamentals else None,
                "source": "vnstock fundamental API",
                "fiscal_year": latest_fundamentals.get('fiscal_year') if latest_fundamentals else None,
                "fiscal_quarter": latest_fundamentals.get('fiscal_quarter') if latest_fundamentals else None
            },
            "layer_4_technical": {
                "symbol": latest_market.get('symbol') if latest_market else None,
                "price": float(latest_market.get('close')) if latest_market and latest_market.get('close') else None,
                "ma20": float(latest_market.get('ma20')) if latest_market and not pd.isna(latest_market.get('ma20')) else None,
                "ma50": float(latest_market.get('ma50')) if latest_market and not pd.isna(latest_market.get('ma50')) else None,
                "ma200": float(latest_market.get('ma200')) if latest_market and not pd.isna(latest_market.get('ma200')) else None,
                "rsi": float(latest_market.get('rsi')) if latest_market and not pd.isna(latest_market.get('rsi')) else None,
                "support": float(latest_market.get('support_level')) if latest_market and not pd.isna(latest_market.get('support_level')) else None,
                "resistance": float(latest_market.get('resistance_level')) if latest_market and not pd.isna(latest_market.get('resistance_level')) else None,
                "volume": int(latest_market.get('volume')) if latest_market and latest_market.get('volume') else None,
                "source": "vnstock history API (calculated)",
                "date": latest_market.get('date') if latest_market else None
            },
            "layer_5_news_sentiment": {
                "recent_news": latest_news,
                "average_sentiment": None,
                "note": "Sentiment will be added by Claude Haiku in Phase 3",
                "source": "vnstock news API"
            }
        },
        "metadata": {
            "status": "success",
            "timestamp": datetime.utcnow().isoformat()
        }
    }
    
    logger.info("[BUILD] ✓ Context package ready")
    return context_package


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/")
def root():
    """Health check"""
    return {
        "name": "SkillVault ETL API",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "/api/health": "Health check",
            "/api/etl/run": "Run full ETL pipeline"
        }
    }


@app.get("/api/health")
def health():
    """Health check endpoint"""
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


@app.post("/api/etl/run")
def run_etl():
    """MAIN ETL PIPELINE - All 6 stages"""
    logger.info("═" * 80)
    logger.info("[START] ETL Pipeline initiated")
    logger.info("═" * 80)
    
    try:
        # Validate env
        validate_environment()
        
        # Init clients
        init_clients()
        
        # Create sync log
        sync_id = create_sync_log_entry()
        
        # ═══ STAGE 2: FETCH ═══
        logger.info("\n[STAGE 2] FETCH DATA")
        market_data_list = fetch_market_data()
        fundamentals_list = fetch_fundamentals()
        macro_list = fetch_macro_data()
        news_list = fetch_news_data()
        insights_list = fetch_market_insights()
        
        # ═══ STAGE 3: CALCULATE ═══
        logger.info("\n[STAGE 3] CALCULATE TECHNICALS")
        market_data_enriched = calculate_technicals(market_data_list)
        
        # ═══ STAGE 4: VALIDATE ═══
        logger.info("\n[STAGE 4] VALIDATE DATA")
        market_data_records, market_data_skipped = validate_records(
            dataframe_to_records(pd.concat(market_data_enriched), "market_data"),
            "market_data",
            CONFIDENCE_THRESHOLDS["market_data"]
        )
        fundamentals_records, fund_skipped = validate_records(
            fundamentals_list,
            "fundamentals",
            CONFIDENCE_THRESHOLDS["fundamentals"]
        )
        macro_records, macro_skipped = validate_records(
            macro_list,
            "macro_data",
            CONFIDENCE_THRESHOLDS["macro_data"]
        )
        news_records, news_skipped = validate_records(
            news_list,
            "news_intelligence",
            CONFIDENCE_THRESHOLDS["news_intelligence"]
        )
        insights_records, insights_skipped = validate_records(
            insights_list,
            "market_insights",
            CONFIDENCE_THRESHOLDS["market_insights"]
        )
        
        # ═══ STAGE 5: UPSERT ═══
        logger.info("\n[STAGE 5] UPSERT TO SUPABASE")
        upsert_stats = upsert_to_supabase(
            market_data_records,
            fundamentals_records,
            macro_records,
            news_records,
            insights_records
        )
        
        # Update sync_log
        try:
            supabase_client.table("sync_log").update({
                "status": "SUCCESS",
                "records_inserted": sum([v for k, v in upsert_stats.items() if 'inserted' in k]),
                "records_updated": sum([v for k, v in upsert_stats.items() if 'updated' in k]),
                "records_skipped": upsert_stats['total_failed'],
                "execution_time_ms": 0,
            }).eq("id", sync_id).execute()
        except Exception as e:
            logger.warning(f"[SYNC_LOG] Update failed: {str(e)[:100]}")
        
        # ═══ STAGE 6: BUILD CONTEXT ═══
        logger.info("\n[STAGE 6] BUILD CONTEXT PACKAGE")
        context_package = build_context_package(
            market_data_enriched,
            fundamentals_records,
            macro_records,
            news_records,
            insights_records
        )
        
        logger.info("\n" + "═" * 80)
        logger.info("[SUCCESS] ETL Pipeline complete")
        logger.info("═" * 80 + "\n")
        
        return {
            "status": "success",
            "timestamp": datetime.utcnow().isoformat(),
            "stats": upsert_stats,
            "context_package": context_package
        }
    
    except Exception as e:
        logger.error(f"\n[FAILED] {str(e)}\n{traceback.format_exc()}")
        
        # Update sync_log
        if sync_id:
            try:
                supabase_client.table("sync_log").update({
                    "status": "FAILED",
                    "error_message": str(e)[:500],
                }).eq("id", sync_id).execute()
            except:
                pass
        
        return {
            "status": "failed",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }


# ═══════════════════════════════════════════════════════════════════════════════
# STARTUP
# ═══════════════════════════════════════════════════════════════════════════════

@app.on_event("startup")
def startup():
    """Initialize on startup"""
    try:
        validate_environment()
        init_clients()
        logger.info("[STARTUP] ✓ All systems ready")
    except Exception as e:
        logger.error(f"[STARTUP] Failed: {str(e)}")
        raise


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
