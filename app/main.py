from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker


def _must_get_env(name: str) -> str:
    v = os.getenv(name)
    if not v or not v.strip():
        raise RuntimeError(f"Missing required env var: {name}")
    return v.strip()


DATABASE_URL = _must_get_env("DATABASE_URL")

# Пример ожидаемого формата:
# postgresql+asyncpg://USER:PASSWORD@HOST:PORT/DBNAME

engine: AsyncEngine = create_async_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    future=True,
)
SessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class RawIngest(Base):
    __tablename__ = "raw_ingest"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(index=True)  # "wb"
    kind: Mapped[str] = mapped_column(index=True)    # "ads_stats" | "sales_funnel" | "search_queries" etc
    shop_id: Mapped[Optional[str]] = mapped_column(index=True, nullable=True)
    occurred_at_ms: Mapped[int] = mapped_column(index=True)  # timestamp from sender (or now)
    received_at_ms: Mapped[int] = mapped_column(index=True)
    payload: Mapped[Dict[str, Any]] = mapped_column(JSONB)


class AdChangeEvent(Base):
    __tablename__ = "ad_change_events"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    shop_id: Mapped[Optional[str]] = mapped_column(index=True, nullable=True)
    campaign_id: Mapped[str] = mapped_column(index=True)
    action: Mapped[str] = mapped_column(index=True)  # "enable" | "disable" | "bid_set" | "kw_add" | "kw_remove"
    actor: Mapped[str] = mapped_column(index=True)   # "n8n" | "ui" | "system"
    occurred_at_ms: Mapped[int] = mapped_column(index=True)
    meta: Mapped[Dict[str, Any]] = mapped_column(JSONB)


async def ensure_schema() -> None:
    # Минимально-надёжная автосхема для старта (позже заменим на миграции Alembic).
    async with engine.begin() as conn:
        # Проверка соединения
        await conn.execute(text("SELECT 1"))

        # Создание таблиц
        await conn.run_sync(Base.metadata.create_all)

        # Индексы (частично уже есть через index=True, но добавим полезные)
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_raw_ingest_source_kind_time ON raw_ingest (source, kind, occurred_at_ms)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ad_change_campaign_time ON ad_change_events (campaign_id, occurred_at_ms)"))


class HealthResponse(BaseModel):
    ok: bool
    ts_ms: int
    db: str


class RawIngestRequest(BaseModel):
    source: str = Field(..., examples=["wb"])
    kind: str = Field(..., examples=["ads_stats"])
    shop_id: Optional[str] = Field(None, examples=["shop_1"])
    occurred_at_ms: Optional[int] = Field(None, description="timestamp from sender in ms; if omitted -> now")
    payload: Dict[str, Any]


class RawIngestResponse(BaseModel):
    id: int
    received_at_ms: int


class AdChangeEventRequest(BaseModel):
    shop_id: Optional[str] = Field(None, examples=["shop_1"])
    campaign_id: str = Field(..., examples=["123456"])
    action: str = Field(..., examples=["enable"])
    actor: str = Field(..., examples=["n8n"])
    occurred_at_ms: Optional[int] = None
    meta: Dict[str, Any] = Field(default_factory=dict)


class AdChangeEventResponse(BaseModel):
    id: int
    occurred_at_ms: int


app = FastAPI(title="SRVWB Core API", version="0.1.0")


@app.on_event("startup")
async def _startup() -> None:
    await ensure_schema()


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    ts = int(time.time() * 1000)
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        db = "ok"
    except Exception as e:
        db = f"error: {type(e).__name__}"
    return HealthResponse(ok=(db == "ok"), ts_ms=ts, db=db)


@app.post("/ingest/raw", response_model=RawIngestResponse)
async def ingest_raw(req: RawIngestRequest) -> RawIngestResponse:
    received = int(time.time() * 1000)
    occurred = req.occurred_at_ms if req.occurred_at_ms is not None else received

    row = RawIngest(
        source=req.source,
        kind=req.kind,
        shop_id=req.shop_id,
        occurred_at_ms=int(occurred),
        received_at_ms=received,
        payload=req.payload,
    )

    async with SessionLocal() as session:
        session.add(row)
        await session.commit()
        await session.refresh(row)

    return RawIngestResponse(id=row.id, received_at_ms=received)


@app.post("/ads/change_event", response_model=AdChangeEventResponse)
async def ads_change_event(req: AdChangeEventRequest) -> AdChangeEventResponse:
    ts = int(time.time() * 1000)
    occurred = req.occurred_at_ms if req.occurred_at_ms is not None else ts

    if req.action not in {"enable", "disable", "bid_set", "kw_add", "kw_remove"}:
        raise HTTPException(status_code=400, detail="Invalid action")

    row = AdChangeEvent(
        shop_id=req.shop_id,
        campaign_id=req.campaign_id,
        action=req.action,
        actor=req.actor,
        occurred_at_ms=int(occurred),
        meta=req.meta,
    )

    async with SessionLocal() as session:
        session.add(row)
        await session.commit()
        await session.refresh(row)

    return AdChangeEventResponse(id=row.id, occurred_at_ms=int(occurred))
