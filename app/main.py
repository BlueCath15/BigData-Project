"""
Fraud Detection System — API principal.

Arranca con:
    pip install -r requirements.txt
    python -m uvicorn app.main:app --reload --reload-exclude "app/data/*"
    https://vigilant-eureka-qwwg95g7xr5cp44-8000.github.dev/docs
"""

from fastapi import FastAPI

from app.documents.routers import router
from app.streaming.routers import router as streaming_router

import os
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

app = FastAPI(title="Fraud Detection System API")

app.include_router(router)
app.include_router(streaming_router)


@app.get("/")
def root():
    return {
        "message": "Fraud Detection API running"
    }


@app.get("/health")
def health_check():
    return {
        "status": "ok"
    }