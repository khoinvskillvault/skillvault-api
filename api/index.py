from fastapi import FastAPI
from datetime import datetime

app = FastAPI()

@app.get("/")
def root():
    return {"status": "running", "version": "2.0.0"}

@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/api/stock/{symbol}")
def get_stock(symbol: str):
    return {
        "symbol": symbol.upper(),
        "message": "Coming soon with Silver data"
    }

@app.post("/api/etl/run")
def run_etl():
    return {
        "status": "success",
        "message": "ETL ready"
    }

handler = app
