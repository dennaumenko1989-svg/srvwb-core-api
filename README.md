# SRVWB Core API

## Endpoints
- GET /health
- POST /ingest/raw
- POST /ads/change_event

## Required env
- DATABASE_URL = postgresql+asyncpg://USER:PASSWORD@HOST:PORT/DBNAME

## Run locally
pip install -r requirements.txt
export DATABASE_URL="postgresql+asyncpg://..."
uvicorn app.main:app --host 0.0.0.0 --port 8000
