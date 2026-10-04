from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException, Query, status
from fastapi.responses import StreamingResponse

from execledger.models import (
    TERMINAL_STATUSES,
    AttemptRecord,
    EffectRecord,
    ExecutionRecord,
    ExecutionSpec,
    SnapshotRecord,
    SubmitResult,
)
from execledger.service import ExecutionService
from execledger.store import IdempotencyConflict


def create_app(
    root: Path | None = None,
    *,
    workers: int = 2,
    lease_seconds: float = 5.0,
    heartbeat_interval: float | None = None,
    worker_id: str | None = None,
) -> FastAPI:
    state_root = root or Path(os.environ.get("EXECLEDGER_ROOT", ".execledger"))
    service = ExecutionService(
        state_root,
        workers=workers,
        lease_seconds=lease_seconds,
        heartbeat_interval=heartbeat_interval,
        worker_id=worker_id,
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await service.start()
        try:
            yield
        finally:
            await service.stop()
            service.store.close()

    app = FastAPI(title="ExecLedger", version="0.3.0", lifespan=lifespan)
    app.state.service = service

    @app.get("/healthz")
    async def healthz() -> dict[str, object]:
        return {
            "ok": True,
            "workers": workers,
            "worker_id": service.runner.worker_id,
            "lease_seconds": lease_seconds,
        }

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

    @app.get("/v1/executions/{execution_id}/attempts", response_model=list[AttemptRecord])
    async def get_attempts(execution_id: str):
        try:
            return service.store.attempts(execution_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="execution not found") from exc

    @app.get("/v1/executions/{execution_id}/effects", response_model=list[EffectRecord])
    async def get_effects(
        execution_id: str,
        after: int = Query(default=0, ge=0),
        limit: int = Query(default=1000, ge=1, le=5000),
    ):
        try:
            return service.store.effects_after(execution_id, after, limit=limit)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="execution not found") from exc

    @app.get("/v1/executions/{execution_id}/events")
    async def stream_events(
        execution_id: str,
        after: int = Query(default=0, ge=0),
        last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
    ):
        try:
            service.store.get(execution_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="execution not found") from exc

        cursor = after
        if last_event_id is not None:
            try:
                cursor = max(cursor, int(last_event_id))
            except ValueError as exc:
                raise HTTPException(status_code=400, detail="invalid Last-Event-ID") from exc
            if cursor < 0:
                raise HTTPException(status_code=400, detail="invalid Last-Event-ID")

        start_cursor = cursor

        async def event_source():
            cursor = start_cursor
            while True:
                batch = service.store.effects_after(execution_id, cursor, limit=500)
                if batch:
                    for effect in batch:
                        cursor = effect.seq
                        payload = json.dumps(
                            effect.model_dump(mode="json"),
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                        yield (
                            f"id: {effect.seq}\n"
                            f"event: {effect.kind}\n"
                            f"data: {payload}\n\n"
                        )
                    continue

                record = service.store.get(execution_id)
                if record.status in TERMINAL_STATUSES:
                    return
                yield ": keep-alive\n\n"
                await asyncio.sleep(0.25)

        return StreamingResponse(
            event_source(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache"},
        )

    @app.get("/v1/executions/{execution_id}/snapshots", response_model=list[SnapshotRecord])
    async def get_snapshots(execution_id: str):
        try:
            return service.store.snapshots(execution_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="execution not found") from exc

    return app


app = create_app()
