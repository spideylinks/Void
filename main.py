"""
Void MCU Streaming Bridge v3.2 (Safe Startup & Peer Resolution)
Telegram -> HTTP Range streaming (FastAPI + Pyrogram)
"""

import asyncio
import logging
import mimetypes
import os
import re
import secrets
import time
import uuid
from collections import OrderedDict
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator, Optional
from urllib.parse import quote

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pyrogram import Client
from pyrogram.errors import FloodWait

# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("VoidServer")

TG_CHUNK = 1024 * 1024  # Pyrogram stream_media works in 1 MiB chunks


# --------------------------------------------------------------------------- #
# Config (validated at startup -> fail fast)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Settings:
    api_id: int
    api_hash: str
    bot_token: str
    chat_id: int
    stream_token: str
    max_streams: int
    cache_ttl: int
    cache_size: int
    allowed_origins: list

    @classmethod
    def load(cls) -> "Settings":
        def req(name: str) -> str:
            val = os.environ.get(name, "").strip()
            if not val:
                raise RuntimeError(f"Missing required env var: {name}")
            return val

        try:
            api_id = int(req("API_ID"))
            chat_id = int(req("CHAT_ID"))
        except ValueError as e:
            raise RuntimeError(f"API_ID / CHAT_ID must be integers: {e}")

        origins = [o.strip() for o in os.environ.get("ALLOWED_ORIGINS", "*").split(",") if o.strip()]
        return cls(
            api_id=api_id,
            api_hash=req("API_HASH"),
            bot_token=req("BOT_TOKEN"),
            chat_id=chat_id,
            stream_token=os.environ.get("STREAM_TOKEN", "").strip(),
            max_streams=int(os.environ.get("MAX_STREAMS", "20")),
            cache_ttl=int(os.environ.get("CACHE_TTL", "600")),
            cache_size=int(os.environ.get("CACHE_SIZE", "256")),
            allowed_origins=origins or ["*"],
        )


settings = Settings.load()

bot = Client(
    "mcu_streamer_session",
    api_id=settings.api_id,
    api_hash=settings.api_hash,
    bot_token=settings.bot_token,
    in_memory=True,
)

stream_slots = asyncio.Semaphore(settings.max_streams)
active_streams = 0
STARTED_AT = time.time()


# --------------------------------------------------------------------------- #
# Media metadata cache (TTL + LRU)
# --------------------------------------------------------------------------- #
@dataclass
class MediaInfo:
    message: object
    size: int
    name: str
    mime: str
    etag: str


class TTLCache:
    def __init__(self, ttl: int, max_size: int):
        self.ttl, self.max_size = ttl, max_size
        self._data: "OrderedDict[int, tuple[float, MediaInfo]]" = OrderedDict()

    def get(self, key: int) -> Optional[MediaInfo]:
        item = self._data.get(key)
        if not item:
            return None
        expires, value = item
        if expires < time.monotonic():
            self._data.pop(key, None)
            return None
        self._data.move_to_end(key)
        return value

    def put(self, key: int, value: MediaInfo) -> None:
        self._data[key] = (time.monotonic() + self.ttl, value)
        self._data.move_to_end(key)
        while len(self._data) > self.max_size:
            self._data.popitem(last=False)

    def invalidate(self, key: int) -> None:
        self._data.pop(key, None)


cache = TTLCache(settings.cache_ttl, settings.cache_size)


# --------------------------------------------------------------------------- #
# App lifecycle
# --------------------------------------------------------------------------- #
@asynccontextmanager
async def lifespan(_: FastAPI):
    logger.info("Starting Telegram client...")
    await bot.start()
    me = await bot.get_me()
    logger.info("Connected as @%s (ID: %s) | chat_id=%s", me.username, me.id, settings.chat_id)
    
    # Safe pre-warm: won't crash deployment if bot isn't in group yet
    try:
        chat = await bot.get_chat(settings.chat_id)
        logger.info("Successfully resolved & cached chat: %s (Type: %s)", getattr(chat, "title", "Private"), chat.type)
    except Exception as e:
        logger.warning("WARNING: Could not pre-resolve chat %s. Make sure bot is added as admin in the group! Error: %s", settings.chat_id, e)

    try:
        yield
    finally:
        logger.info("Shutting down Telegram client...")
        await bot.stop()


app = FastAPI(title="Void MCU Streaming Bridge", version="3.2", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_methods=["GET", "HEAD", "OPTIONS"],
    allow_headers=["Range", "X-API-Key", "If-None-Match"],
    expose_headers=["Content-Range", "Accept-Ranges", "Content-Length", "ETag"],
)


@app.middleware("http")
async def request_context(request: Request, call_next):
    rid = uuid.uuid4().hex[:8]
    t0 = time.perf_counter()
    response = await call_next(request)
    response.headers["X-Request-ID"] = rid
    logger.info(
        "[%s] %s %s -> %s (%.0f ms)",
        rid, request.method, request.url.path, response.status_code,
        (time.perf_counter() - t0) * 1000,
    )
    return response


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def check_auth(request: Request, token: Optional[str]) -> None:
    if not settings.stream_token:
        return
    supplied = token or request.headers.get("x-api-key", "")
    if not secrets.compare_digest(supplied, settings.stream_token):
        raise HTTPException(status_code=401, detail="Unauthorized")


async def resolve_media(message_id: int, force: bool = False) -> MediaInfo:
    if not force:
        cached = cache.get(message_id)
        if cached:
            return cached

    try:
        msg = await bot.get_messages(settings.chat_id, message_id)
    except FloodWait as fw:
        raise HTTPException(
            status_code=429, detail="Telegram rate limit, retry later",
            headers={"Retry-After": str(int(fw.value))},
        )

    if not msg or msg.empty:
        raise HTTPException(status_code=404, detail="Message not found")

    media = msg.video or msg.document or msg.audio
    if not media:
        raise HTTPException(status_code=400, detail="Message has no streamable media")

    name = getattr(media, "file_name", None) or f"media_{message_id}"
    mime = (
        getattr(media, "mime_type", None)
        or mimetypes.guess_type(name)[0]
        or "application/octet-stream"
    )
    info = MediaInfo(
        message=msg,
        size=media.file_size,
        name=name,
        mime=mime,
        etag=f'"{media.file_unique_id}"',
    )
    cache.put(message_id, info)
    return info


_RANGE_RE = re.compile(r"^bytes=(\d*)-(\d*)$")


def parse_range(header: str, size: int) -> tuple:
    unsatisfiable = HTTPException(
        status_code=416,
        detail="Requested range not satisfiable",
        headers={"Content-Range": f"bytes */{size}"},
    )
    m = _RANGE_RE.match(header.strip().split(",")[0])
    if not m:
        raise unsatisfiable
    s, e = m.groups()

    if s == "" and e == "":
        raise unsatisfiable
    if s == "":
        n = int(e)
        if n == 0:
            raise unsatisfiable
        start, end = max(size - n, 0), size - 1
    else:
        start = int(s)
        end = int(e) if e else size - 1

    if start >= size or start > end:
        raise unsatisfiable
    return start, min(end, size - 1)


async def iter_byte_range(message, start: int, end: int) -> AsyncIterator[bytes]:
    first_chunk = start // TG_CHUNK
    last_chunk = end // TG_CHUNK
    skip = start - first_chunk * TG_CHUNK
    remaining = end - start + 1

    async for chunk in bot.stream_media(
        message, limit=last_chunk - first_chunk + 1, offset=first_chunk
    ):
        if skip:
            chunk = chunk[skip:]
            skip = 0
        if len(chunk) > remaining:
            chunk = chunk[:remaining]
        if chunk:
            remaining -= len(chunk)
            yield chunk
        if remaining <= 0:
            break


# --------------------------------------------------------------------------- #
# Routes
# --------------------------------------------------------------------------- #
@app.get("/")
async def root():
    return {"status": "Void MCU Streamer Bridge is Active", "version": app.version}


@app.get("/health")
async def health():
    connected = bot.is_connected
    body = {
        "status": "ok" if connected else "degraded",
        "telegram_connected": connected,
        "active_streams": active_streams,
        "max_streams": settings.max_streams,
        "uptime_sec": int(time.time() - STARTED_AT),
    }
    return JSONResponse(body, status_code=200 if connected else 503)


@app.api_route("/stream", methods=["GET", "HEAD"])
async def stream_video(
    request: Request,
    message_id: int = Query(..., gt=0),
    token: Optional[str] = Query(None),
):
    global active_streams
    check_auth(request, token)

    info = await resolve_media(message_id)

    if request.headers.get("if-none-match") == info.etag:
        return Response(status_code=304, headers={"ETag": info.etag})

    range_header = request.headers.get("range")
    if range_header:
        start, end = parse_range(range_header, info.size)
        status = 206
    else:
        start, end, status = 0, info.size - 1, 200
    length = end - start + 1

    headers = {
        "Accept-Ranges": "bytes",
        "Content-Length": str(length),
        "Content-Type": info.mime,
        "ETag": info.etag,
        "Cache-Control": "public, max-age=3600",
        "Content-Disposition": f"inline; filename*=UTF-8''{quote(info.name)}",
    }
    if status == 206:
        headers["Content-Range"] = f"bytes {start}-{end}/{info.size}"

    if request.method == "HEAD":
        return Response(status_code=status, headers=headers)

    if stream_slots.locked():
        raise HTTPException(
            status_code=503, detail="Server busy, try again shortly",
            headers={"Retry-After": "5"},
        )
    await stream_slots.acquire()
    active_streams += 1

    async def body() -> AsyncIterator[bytes]:
        global active_streams
        sent = 0
        try:
            async for chunk in iter_byte_range(info.message, start, end):
                if await request.is_disconnected():
                    logger.info("Client disconnected (msg %s) after %d bytes", message_id, sent)
                    return
                sent += len(chunk)
                yield chunk
        except asyncio.CancelledError:
            logger.info("Stream cancelled (msg %s) after %d bytes", message_id, sent)
            raise
        except FloodWait as fw:
            logger.warning("FloodWait %ss during stream (msg %s)", fw.value, message_id)
            cache.invalidate(message_id)
        except Exception:
            logger.exception("Stream failed (msg %s) after %d/%d bytes", message_id, sent, length)
            cache.invalidate(message_id)
        finally:
            active_streams -= 1
            stream_slots.release()

    logger.info("Serving msg %s | %s | bytes %d-%d/%d", message_id, info.name, start, end, info.size)
    return StreamingResponse(body(), status_code=status, headers=headers)


@app.exception_handler(Exception)
async def unhandled(_: Request, exc: Exception):
    logger.exception("Unhandled error: %s", exc)
    return JSONResponse({"detail": "Internal server error"}, status_code=500)
