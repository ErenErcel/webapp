#
# CONFIGURATION: Ortam değişkenleri ve Elasticsearch bağlantısı
from fastapi import FastAPI, Depends, Request, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, Any, List
from sqlalchemy import Column, String, DateTime, text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Session
from datetime import datetime, timezone
import uuid, os, asyncio, logging, json
import time
from sqlalchemy.exc import OperationalError
from elasticsearch import Elasticsearch
from app.db import Base, engine, SessionLocal

INSTANCE_NAME = os.getenv("INSTANCE_NAME", "unknown")

# Elasticsearch config (optional)
ELASTIC_URL = os.getenv("ELASTIC_URL")
ELASTIC_INDEX = os.getenv("ELASTIC_INDEX", "events")
es = Elasticsearch([ELASTIC_URL], request_timeout=5, retry_on_timeout=True, max_retries=2) if ELASTIC_URL else None

def es_available() -> bool:
    if not es:
        return False
    try:
        return bool(es.ping())
    except Exception:
        return False

def es_index_event(e: "Event") -> None:
    """Best-effort: index a row into Elasticsearch if available."""
    if not es_available():
        return
    try:
        es.index(index=ELASTIC_INDEX, id=str(e.id), document={
            "id": str(e.id),
            "source": e.source,
            "type": e.type,
            "payload": e.payload,
            # Ensure created_at is stored in ES as UTC with timezone info
            # Kibana expects time fields to be proper date types; naive datetimes are treated as UTC
            # and may appear outside the default time picker. We always index with explicit Z.
            **({
                "created_at":
                (
                    (
                        (_ts := (
                            e.created_at
                            if not (e.created_at is None)
                            else None
                        )) and (
                            (
                                (_ts if _ts.tzinfo is not None else
                                 (_ts - (datetime.now() - datetime.utcnow())).replace(tzinfo=timezone.utc)
                                ).astimezone(timezone.utc)
                            ).isoformat()
                        )
                    )
                    if e.created_at else None
                )
            }),
            "instance": e.instance,
        })
    except Exception as ex:
        logging.warning(f"ES index error: {ex}")

# UYGULAMA BAŞLATMA: FastAPI uygulama ve CORS middleware
app = FastAPI(title="Olay Kayıt Servisi", version="0.1.0")

# Allow all origins for demo purposes; tighten in production
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# VERİTABANI MODELİ: Event tablosu tanımı
class Event(Base):
    __tablename__ = "events"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source = Column(String(100), nullable=False)
    type = Column(String(100), nullable=False)
    payload = Column(JSONB, nullable=True)
    created_at = Column(DateTime, server_default=text("CURRENT_TIMESTAMP"))
    instance = Column(String(50), nullable=False, default=INSTANCE_NAME)

# STARTUP: Uygulama başlatıldığında veritabanı ve ES hazırlığı
@app.on_event("startup")
async def on_startup():
    async def _init_db():
        # Tek instance şema kurulum yarışı olmaması için advisory lock kullan
        # ve Postgres hazır değilse bir süre retry et (background)
        for _ in range(60):
            try:
                with engine.begin() as conn:
                    # global bir lock (uydurma bir sayı yeterli)
                    conn.exec_driver_sql("SELECT pg_advisory_lock(1234567890)")
                    try:
                        Base.metadata.create_all(bind=engine)
                        # Indexes for faster queries on listing and JSONB search
                        conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_events_created_at ON events (created_at DESC)")
                        conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_events_payload_gin ON events USING GIN (payload)")
                        # Ensure Elasticsearch index exists (best-effort)
                        if es_available():
                            try:
                                if not es.indices.exists(index=ELASTIC_INDEX):
                                    es.indices.create(index=ELASTIC_INDEX)
                            except Exception as _ex:
                                logging.warning(f"ES index ensure failed: {_ex}")
                        else:
                            logging.info("Elasticsearch not reachable; skipping index ensure.")
                    finally:
                        conn.exec_driver_sql("SELECT pg_advisory_unlock(1234567890)")
                logging.info("DB initialization completed.")
                return
            except OperationalError:
                await asyncio.sleep(1.0)
                continue
            except Exception:
                logging.exception("Unexpected error during DB init; retrying...")
                await asyncio.sleep(1.0)
                continue
        logging.error("DB initialization timed out; service will run but /ready will fail until DB is up.")

    # Non-blocking: schedule background task and return immediately
    asyncio.create_task(_init_db())

# SCHEMA: API input/output modelleri
class EventIn(BaseModel):
    source: str = Field(..., examples=["web", "mobile", "scheduler"])
    type: str = Field(..., examples=["LOGIN", "ORDER_CREATED", "ERROR"])
    payload: Optional[Any] = Field(None, description="Serbest JSON")

class EventOut(BaseModel):
    id: uuid.UUID
    source: str
    type: str
    payload: Optional[Any]
    created_at: Optional[datetime]
    instance: str

    model_config = {
        "from_attributes": True,
    }

# DB OTURUM: SQLAlchemy session dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# MIDDLEWARE: HTTP isteklerini logla ve header ekle
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    try:
        response = await call_next(request)
    except Exception as exc:
        # Ensure instance header is still set on errors
        response = JSONResponse(status_code=500, content={"detail": "Internal Server Error"})
        raise
    finally:
        duration_ms = int((time.time() - start) * 1000)
        print({
            "instance": INSTANCE_NAME,
            "method": request.method,
            "path": request.url.path,
            "client": request.client.host if request.client else None,
            "duration_ms": duration_ms,
            # status may not exist if exception before response; guard
        })
    response.headers["X-Instance"] = INSTANCE_NAME
    response.headers["X-Response-Time-ms"] = str(duration_ms)
    return response

# ENDPOINTLER: API ve yardımcı endpointler
@app.get("/", response_class=HTMLResponse)
async def index():
    return f"""
    <html>
      <head><title>Olay Kayıt Servisi</title></head>
      <body style='font-family: system-ui; padding: 24px;'>
        <h1>Olay Kayıt Servisi</h1>
        <p>Bu örnek servis, gelen olayları Postgres'e yazar ve 3 kopya halinde Nginx üzerinden yük dengelemesi ile yayınlanır.</p>
        <ul>
          <li>POST <code>/events</code> → olay kaydı ekler</li>
          <li>GET <code>/events</code> → son olayları listeler</li>
          <li>GET <code>/events/{{id}}</code> → tekil olayı döner</li>
          <li>GET <code>/health</code> → canlılık (liveness)</li>
          <li>GET <code>/ready</code> → veritabanı hazır mı (readiness)</li>
          <li>GET <code>/dashboard</code> → mini HTML dashboard</li>
        </ul>
        <p>Aktif instance: <b>{INSTANCE_NAME}</b></p>
        <p><a href='/docs'>OpenAPI Docs</a> · <a href='/dashboard'>Dashboard</a></p>
      </body>
    </html>
    """


@app.get("/health")
async def health():
    return {"status": "ok", "instance": INSTANCE_NAME}


# DEBUG: Elasticsearch bağlantı durumu
@app.get("/debug/es")
async def debug_es():
    info = {"ELASTIC_URL": ELASTIC_URL, "ELASTIC_INDEX": ELASTIC_INDEX, "client_created": bool(es)}
    try:
        info["ping"] = es_available()
        if es_available():
            info["cluster"] = es.info()
            info["indices"] = es.indices.get_alias("*")
    except Exception as ex:
        info["error"] = str(ex)
    return info

# DEBUG: Konfigürasyon gösterimi
@app.get("/debug/config")
async def debug_config():
    try:
        url = engine.url
        # SQLAlchemy URL safely as string
        try:
            safe = url.render_as_string(hide_password=True)  # SA 2.x
        except Exception:
            safe = str(url)
        return {
            "instance": INSTANCE_NAME,
            "engine_url": safe,
            "drivername": url.drivername,
            "username": url.username,
            "host": url.host,
            "port": url.port,
            "database": url.database,
        }
    except Exception as e:
        return {"error": str(e)}

# READINESS: Veritabanı hazır mı kontrolü
@app.get("/ready")
async def ready():
    try:
        with engine.connect() as conn:
            conn.exec_driver_sql("SELECT 1")
        return {"status": "ready", "instance": INSTANCE_NAME}
    except OperationalError as exc:
        # surface as 503 so load balancer can mark this instance as unhealthy
        # do NOT leak secrets; include only host/port/database
        url = engine.url
        hint = {
            "host": url.host,
            "port": url.port,
            "database": url.database,
        }
        raise HTTPException(status_code=503, detail={"error": "database not ready", "hint": hint})

# ADMIN: Postgres'ten Elasticsearch'e toplu veri aktarımı
@app.post("/admin/sync_es")
async def admin_sync_es(
    limit: int = 10000,
    since: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """
    Backfill existing events from Postgres into Elasticsearch.
    - limit: max number of rows to process
    - since: ISO datetime string (e.g., 2025-09-07T12:00:00) to only sync newer rows
    """
    if not es_available():
        raise HTTPException(status_code=503, detail="Elasticsearch is not reachable (set ELASTIC_URL)")

    q = db.query(Event)
    if since:
        try:
            dt = datetime.fromisoformat(since)
            q = q.filter(Event.created_at >= dt)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid 'since' datetime; use ISO format, e.g. 2025-09-07T12:00:00")

    rows = q.order_by(Event.created_at.asc()).limit(max(1, min(200000, int(limit)))).all()
    ok = 0
    errors: List[str] = []
    for r in rows:
        try:
            es_index_event(r)
            ok += 1
        except Exception as ex:  # should be swallowed by helper, but keep for safety
            errors.append(str(ex))
    return {"synced": ok, "attempted": len(rows), "errors": errors[:5]}

# OLAY KAYDI: Yeni event ekle
@app.post("/events", response_model=EventOut)
async def create_event(evt: EventIn, db: Session = Depends(get_db)):
    e = Event(source=evt.source, type=evt.type, payload=evt.payload, instance=INSTANCE_NAME)
    try:
        db.add(e)
        db.commit()
        db.refresh(e)
    except Exception:
        db.rollback()
        raise
    # Also index to Elasticsearch (non-blocking if ES missing)
    es_index_event(e)
    # Thanks to model_config.from_attributes=True we can validate from ORM directly
    return EventOut.model_validate(e)

# OLAY LİSTELEME: Son eventleri getir
@app.get("/events", response_model=List[EventOut])
async def list_events(
    limit: int = 50,
    source: Optional[str] = None,
    type: Optional[str] = None,
    q: Optional[str] = None,
    db: Session = Depends(get_db),
):
    limit = max(1, min(500, int(limit)))
    query = db.query(Event)
    if source:
        query = query.filter(Event.source == source)
    if type:
        query = query.filter(Event.type == type)
    if q:
        # simple text match over payload JSON
        query = query.filter(text("payload::text ILIKE :q")).params(q=f"%{q}%")
    rows = query.order_by(Event.created_at.desc()).limit(limit).all()
    return [EventOut.model_validate(r) for r in rows]

# OLAY DETAY: Tekil event getir
@app.get("/events/{event_id}", response_model=EventOut)
async def get_event(event_id: uuid.UUID, db: Session = Depends(get_db)):
    row = db.query(Event).filter(Event.id == event_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="event not found")
    return EventOut.model_validate(row)

# DEMO: Test amaçlı örnek event ekle
@app.get("/demo")
async def demo(db: Session = Depends(get_db)):
    e = Event(source="demo", type="PING", payload={"note": "hello"}, instance=INSTANCE_NAME)
    db.add(e)
    db.commit()
    db.refresh(e)
    if es_available():
        try:
            es.index(index=ELASTIC_INDEX, id=str(e.id), document={
                "id": str(e.id),
                "source": e.source,
                "type": e.type,
                "payload": e.payload,
                # Ensure created_at is stored in ES as UTC with timezone info
                # Kibana expects time fields to be proper date types; naive datetimes are treated as UTC
                # and may appear outside the default time picker. We always index with explicit Z.
                **({
                    "created_at":
                    (
                        (
                            (_ts := (
                                e.created_at
                                if not (e.created_at is None)
                                else None
                            )) and (
                                (
                                    (_ts if _ts.tzinfo is not None else
                                     (_ts - (datetime.now() - datetime.utcnow())).replace(tzinfo=timezone.utc)
                                    ).astimezone(timezone.utc)
                                ).isoformat()
                            )
                        )
                        if e.created_at else None
                    )
                }),
                "instance": e.instance,
            })
        except Exception as ex:
            logging.warning(f"ES index error: {ex}")
    return {"inserted": True, "event": EventOut.model_validate(e).model_dump()}

# DASHBOARD: Mini HTML dashboard arayüzü
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(
    limit: int = 50,
    source: Optional[str] = None,
    type: Optional[str] = None,
    q: Optional[str] = None,
    db: Session = Depends(get_db),
):
    try:
        limit = max(1, min(500, int(limit)))
        query = db.query(Event)
        if source:
            query = query.filter(Event.source == source)
        if type:
            query = query.filter(Event.type == type)
        if q:
            query = query.filter(text("payload::text ILIKE :q")).params(q=f"%{q}%")
        rows = query.order_by(Event.created_at.desc()).limit(limit).all()

        def esc(s: str) -> str:
            import html
            return html.escape(str(s))

        trs = []
        for r in rows:
            # Render payload safely as JSON string
            try:
                payload_str = json.dumps(r.payload, ensure_ascii=False, separators=(",", ": ")) if r.payload is not None else ""
            except Exception:
                payload_str = str(r.payload)
            trs.append(
                f"<tr>"
                f"<td>{esc(r.id)}</td>"
                f"<td>{esc(r.source)}</td>"
                f"<td>{esc(r.type)}</td>"
                f"<td><pre style='margin:0'>{esc(payload_str)}</pre></td>"
                f"<td>{r.created_at.strftime('%Y-%m-%d %H:%M:%S') if r.created_at else ''}</td>"
                f"<td>{esc(r.instance)}</td>"
                f"</tr>"
            )

        return """
        <html>
          <head>
            <meta charset='utf-8' />
            <title>Events Dashboard</title>
            <style>
              body{{font-family: system-ui; margin:20px}}
              table{{border-collapse:collapse; width:100%}}
              th,td{{border:1px solid #ddd; padding:8px; vertical-align:top}}
              th{{background:#f5f5f5; position:sticky; top:0}}
              .controls{{margin-bottom:12px}}
              input,select{{padding:6px; margin-right:6px}}
              code{{background:#f6f8fa; padding:2px 4px; border-radius:4px}}
            </style>
          </head>
          <body>
            <h2>Events Dashboard <small style='font-weight:normal'>(instance: {instance})</small></h2>
            <form class='controls' method='get'>
              <label>source: <input name='source' value='{source}'/></label>
              <label>type: <input name='type' value='{type}'/></label>
              <label>q: <input name='q' value='{q}' placeholder='payload contains...'/></label>
              <label>limit: <input name='limit' type='number' min='1' max='500' value='{limit}'/></label>
              <button type='submit'>Filter</button>
              <a href='/dashboard' style='margin-left:8px'>Reset</a>
            </form>
            <div style='margin-bottom:8px'>
              <code>GET /events?limit={limit}&source={source}&type={type}&q={q}</code>
            </div>
            <table>
              <thead>
                <tr>
                  <th>id</th>
                  <th>source</th>
                  <th>type</th>
                  <th>payload</th>
                  <th>created_at</th>
                  <th>instance</th>
                </tr>
              </thead>
              <tbody>
                {rows}
              </tbody>
            </table>
            <script>setTimeout(()=>location.reload(), 15000);</script>
          </body>
        </html>
        """.format(
            instance=esc(INSTANCE_NAME),
            source=esc(source or ""),
            type=esc(type or ""),
            q=esc(q or ""),
            limit=limit,
            rows="".join(trs),
        )
    except Exception as e:
        # Render simple error page instead of 500 JSON for nicer UX
        return HTMLResponse(
            content=f"""
            <html><body style='font-family: system-ui; padding:20px;'>
            <h2>Dashboard Error</h2>
            <pre>{e}</pre>
            </body></html>
            """,
            status_code=500,
        )