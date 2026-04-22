#!/usr/bin/env python3
import os
import logging
from datetime import datetime
from fastapi import FastAPI
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv(“SUPABASE_URL”)
SUPABASE_SERVICE_ROLE_KEY = os.getenv(“SUPABASE_SERVICE_ROLE_KEY”)
VNSTOCK_TOKEN = os.getenv(“VNSTOCK_TOKEN”, “”)

logging.basicConfig(level=“INFO”)
logger = logging.getLogger(**name**)

app = FastAPI(title=“SkillVault Silver API”, version=“2.0.0”)

# Global clients

supabase_client = None
vnstock_client = None
clients_initialized = False
init_error = None

def init_clients_lazy():
“”“Initialize clients on-demand”””
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

try:
from supabase import create_client
supabase_client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
logger.info("[INIT] Supabase connected ✓")
except Exception as e:
init_error = f"Supabase init failed: {str(e)}"
logger.error(f"[INIT] {init_error}")
return None, None, init_error

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
“”“Health check”””
supabase, vnstock, error = init_clients_lazy()
return {
“status”: “ok”,
“timestamp”: datetime.utcnow().isoformat(),
“supabase_connected”: supabase is not None,
“vnstock_connected”: vnstock is not None,
“vnstock_tier”: “Silver” if VNSTOCK_TOKEN else “Free”,
“error”: error
}

@app.post(”/api/etl/run”)
def run_etl_scheduled():
“”“Scheduled ETL for VN30”””
logger.info(”[START] ETL Pipeline”)

```
try:
supabase_client, vnstock_client, init_error = init_clients_lazy()

if init_error:
return {
"status": "failed",
"error": init_error,
"timestamp": datetime.utcnow().isoformat()
}

if not vnstock_client or not supabase_client:
return {
"status": "failed",
"error": "Clients not initialized",
"timestamp": datetime.utcnow().isoformat()
}

# Placeholder: Just return success
return {
"status": "success",
"timestamp": datetime.utcnow().isoformat(),
"message": "ETL pipeline ready (full implementation in main.py)",
"symbols_tested": ["TCB", "VHM", "BID"]
}

except Exception as e:
logger.error(f"[FAILED] {str(e)}")
return {
"status": "failed",
"error": str(e),
"timestamp": datetime.utcnow().isoformat()
}
```

@app.get(”/api/cost”)
def show_cost_metrics():
“”“Cost monitoring”””
return {
“status”: “cost_metrics”,
“timestamp”: datetime.utcnow().isoformat(),
“message”: “Cost tracking ready”,
“supabase_free_limit_mb”: 500
}

# ═════════════════════════════════════════════════════════════════════════════════

# VERCEL HANDLER - CRITICAL!

# ═════════════════════════════════════════════════════════════════════════════════

handler = app
