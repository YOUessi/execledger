from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException, Query, status

from execledger.models import (
    EffectRecord,
    ExecutionRecord,
    ExecutionSpec,
    SnapshotRecord,
    SubmitResult,
)
from execledger.service import ExecutionService
from execledger.store import IdempotencyConflict


def create_app(root: Path | None = None, *, workers: int = 2) -> FastAPI:
    state_root = root or Path(os.environ.get("EXECLEDGER_ROOT", ".execledger"))
    service = ExecutionService(state_root, workers=workers)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await service.start()
        try:
            yield
        finally:
            await service.stop()
            service.store.close()

    app = FastAPI(title="ExecLedger", version="0.1.0", lifespan=lifespan)
    app.state.service = service

    @app.get("/healthz")
    async def healthz() -> dict[str, object]:
        return {"ok": True, "workers": workers}

    @app.post("/v1/executions", response_model=SubmitResult, status_code=status.HTTP_201_CREATED)
    async def submit_execution(
        spec: ExecutionSpec,
        idempotency_key: str = Header(alias="Idempotency-Key", min_length=1, max_length=200),
    ) -> SubmitResult:
        try:
            return await service.submit(spec, idempotency_key)
        except IdempotencyConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/v1/executions", response_model=list[ExecutionRecord])
    async def list_executions(limit: int = Query(default=100, ge=1, le=500)):
        return service.store.list(limit)

    @app.get("/v1/executions/{execution_id}", response_model=ExecutionRecord)
    async def get_execution(execution_id: str):
        try:
            return service.store.get(execution_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="execution not found") from exc

    @app.post("/v1/executions/{execution_id}/cancel", response_model=ExecutionRecord)
    async def cancel_execution(execution_id: str):
        try:
            return await service.cancel(execution_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="execution not found") from exc

    @app.get("/v1/executions/{execution_id}/effects", response_model=list[EffectRecord])
    async def get_effects(execution_id: str):
        try:
            return service.store.effects(execution_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="execution not found") from exc

    @app.get("/v1/executions/{execution_id}/snapshots", response_model=list[SnapshotRecord])
    async def get_snapshots(execution_id: str):
        try:
            return service.store.snapshots(execution_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="execution not found") from exc

    return app


app = create_app()
