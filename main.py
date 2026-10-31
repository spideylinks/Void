"""
Void MCU Streaming Bridge v4.0
Telegram -> HTTP Range streaming (FastAPI + Kurigram/Pyrogram)

requirements.txt:
    fastapi
    uvicorn
    kurigram
    tgcrypto
    aiofiles

Start command (Render):
    uvicorn main:app --host 0.0.0.0 --port 10000

NOTE: Run with a SINGLE worker (one Telegram session per process).

Environment variables
    required : API_ID, API_HASH, BOT_TOKEN, CHAT_ID
    optional : STREAM_TOKEN        shared secret (?token=... or X-API-Key header)
               MAX_STREAMS         parallel streams allowed            (default 20)
               PREFETCH_CHUNKS     1 MiB chunks buffered per stream    (default 3)
               STALL_TIMEOUT       seconds without data before abort   (default 60)
               WARMUP_MESSAGE_ID   media message used for warm-up      (default: auto)
               CACHE_TTL / CACHE_SIZE / ALLOWED_ORIGINS / LOG_LEVEL

What changed in v4.0
    * Telegram download runs in its own task and is never cancelled when a
      player disconnects. Cancelling it in the middle of creating the media
      connection left a half-built session behind, and every later GetFile
      timed out until the server restarted.
    * Media connection is warmed up at startup.
    * Dead media sessions are dropped automatically if Telegram stalls.
    * Pure ASGI request logger (BaseHTTPMiddleware broke disconnect handling
      and caused the "Too little data for declared Content-Length" noise).
    * Stream slots can no longer leak, If-Range support, clearer errors.
    * New endpoints: /info and /test.
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
from contextlib import aclosing, asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator, Optional
from urllib.parse import quote

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError

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
def env_int(name: str, default: int, minimum: int = 0) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise RuntimeError(f"{name} must be an integer, got {raw!r}")
    return max(value, minimum)


@dataclass(frozen=True)
class Settings:
    api_id: int
    api_hash: str
    bot_token: str
    chat_id: int
    stream_token: str          # empty = auth disabled
    max_streams: int
    prefetch_chunks: int
    stall_timeout: int         # seconds
    warmup_message_id: int     # 0 = pick automatically
    cache_ttl: int             # seconds
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
            max_streams=env_int("MAX_STREAMS", 20, 1),
            prefetch_chunks=env_int("PREFETCH_CHUNKS", 3, 1),
            stall_timeout=env_int("STALL_TIMEOUT", 60, 1),
            warmup_message_id=env_int("WARMUP_MESSAGE_ID", 0, 0),
            cache_ttl=env_int("CACHE_TTL", 600, 1),
            cache_size=env_int("CACHE_SIZE", 256, 1),
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

active_streams = 0
STARTED_AT = time.time()

# Keep strong references to background tasks (asyncio only keeps weak ones).
_bg_tasks: set = set()


def spawn(coro) -> asyncio.Task:
    task = asyncio.get_running_loop().create_task(coro)
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)
    return task


# --------------------------------------------------------------------------- #
# Media metadata cache (TTL + LRU) - avoids a Telegram API call per Range request
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
# Telegram helpers
# --------------------------------------------------------------------------- #
def pick_media(msg):
    """Return the streamable media object of a message, or None."""
    if not msg or getattr(msg, "empty", False):
        return None
    return (
        getattr(msg, "video", None)
        or getattr(msg, "document", None)
        or getattr(msg, "audio", None)
        or getattr(msg, "animation", None)
        or getattr(msg, "voice", None)
        or getattr(msg, "video_note", None)
    )


async def warm_up_media_session() -> None:
    """Open the Telegram media connection at startup.

    The first download creates a separate connection to the file's data
    center. Doing it here means no player can interrupt that step."""
    try:
        msg = None
        if settings.warmup_message_id:
            msg = await bot.get_messages(settings.chat_id, settings.warmup_message_id)
        else:
            batch = await bot.get_messages(settings.chat_id, list(range(1, 41)))
            for candidate in batch or []:
                if pick_media(candidate):
                    msg = candidate
                    break
        if not pick_media(msg):
            logger.info("Media warm-up skipped: no media message found")
            return
        agen = bot.stream_media(msg, limit=1)
        try:
            async for _ in agen:
                break
        finally:
            await agen.aclose()
        logger.info("Media session warm-up done")
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.warning("Media warm-up skipped: %s", exc)


_reset_lock = asyncio.Lock()
_last_reset = 0.0


async def reset_media_sessions(reason: str) -> None:
    """Drop Pyrogram's cached media connections so the next download builds a
    fresh one. Used when Telegram stops answering (self-healing)."""
    global _last_reset
    async with _reset_lock:
        now = time.monotonic()
        if now - _last_reset < 20:  # do not thrash
            return
        _last_reset = now
        sessions = getattr(bot, "media_sessions", None)
        if not isinstance(sessions, dict) or not sessions:
            return
        logger.warning("Resetting %d Telegram media session(s): %s", len(sessions), reason)
        for dc_id, sess in list(sessions.items()):
            sessions.pop(dc_id, None)
            try:
                await asyncio.wait_for(sess.stop(), timeout=10)
            except Exception as exc:
                logger.debug("Stopping media session DC%s failed: %s", dc_id, exc)


# --------------------------------------------------------------------------- #
# App lifecycle
# --------------------------------------------------------------------------- #
@asynccontextmanager
async def lifespan(_: FastAPI):
    logger.info("Starting Telegram client...")
    await bot.start()  # fail fast: if this raises, the deploy crashes visibly
    me = await bot.get_me()
    logger.info("Connected as @%s (ID: %s) | chat_id=%s", me.username, me.id, settings.chat_id)
    try:
        chat = await bot.get_chat(settings.chat_id)
        logger.info("Storage chat resolved: %s", getattr(chat, "title", None) or chat.id)
    except Exception as exc:
        logger.error(
            "Could not resolve CHAT_ID %s: %s (is the bot a member/admin of that chat?)",
            settings.chat_id, exc,
        )
    spawn(warm_up_media_session())
    try:
        yield
    finally:
        logger.info("Shutting down Telegram client...")
        tasks = list(_bg_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await bot.stop()


app = FastAPI(title="Void MCU Streaming Bridge", version="4.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_methods=["GET", "HEAD", "OPTIONS"],
    allow_headers=["Range", "If-Range", "X-API-Key", "If-None-Match"],
    expose_headers=["Content-Range", "Accept-Ranges", "Content-Length", "ETag", "X-Request-ID"],
)


class RequestContextMiddleware:
    """Pure ASGI middleware: request id, access log with the real stream
    duration, and a clean connection drop when a stream ends short."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        rid = uuid.uuid4().hex[:8]
        t0 = time.perf_counter()
        is_head = scope.get("method") == "HEAD"
        state = {"status": None, "declared": None, "sent": 0, "truncated": False}

        async def send_wrapper(message):
            kind = message["type"]
            if kind == "http.response.start":
                state["status"] = message["status"]
                headers = list(message.get("headers", []))
                for key, value in headers:
                    if key.lower() == b"content-length":
                        try:
                            state["declared"] = int(value)
                        except ValueError:
                            pass
                headers.append((b"x-request-id", rid.encode()))
                message = {**message, "headers": headers}
            elif kind == "http.response.body":
                state["sent"] += len(message.get("body", b""))
                final = not message.get("more_body", False)
                if (
                    final
                    and not is_head
                    and state["declared"] is not None
                    and state["sent"] < state["declared"]
                ):
                    # Stream ended early. Do not send a bogus end-of-message;
                    # the server closes the connection so the player retries.
                    state["truncated"] = True
                    return
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            logger.info(
                "[%s] %s %s -> %s (%.0f ms%s)",
                rid, scope.get("method"), scope.get("path"), state["status"] or "-",
                (time.perf_counter() - t0) * 1000,
                ", truncated" if state["truncated"] else "",
            )


app.add_middleware(RequestContextMiddleware)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def check_auth(request: Request, token: Optional[str]) -> None:
    if not settings.stream_token:
        return
    supplied = token or request.headers.get("x-api-key", "")
    if not secrets.compare_digest(supplied.encode(), settings.stream_token.encode()):
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
    except RPCError as exc:
        logger.error("Telegram error while resolving msg %s: %s", message_id, exc)
        raise HTTPException(status_code=502, detail=f"Telegram error: {exc.__class__.__name__}")
    except (ValueError, KeyError) as exc:
        logger.error("Storage chat not reachable (msg %s): %s", message_id, exc)
        raise HTTPException(
            status_code=502,
            detail="Storage chat not reachable (check CHAT_ID and bot admin rights)",
        )

    if not msg or getattr(msg, "empty", False):
        raise HTTPException(status_code=404, detail="Message not found")

    media = pick_media(msg)
    if not media:
        raise HTTPException(status_code=400, detail="Message has no streamable media")

    size = int(getattr(media, "file_size", 0) or 0)
    if size <= 0:
        raise HTTPException(status_code=422, detail="Media size is unknown")

    name = getattr(media, "file_name", None) or f"media_{message_id}"
    mime = (
        getattr(media, "mime_type", None)
        or mimetypes.guess_type(name)[0]
        or "application/octet-stream"
    )
    info = MediaInfo(
        message=msg,
        size=size,
        name=name,
        mime=mime,
        etag=f'"{media.file_unique_id}"',
    )
    cache.put(message_id, info)
    return info


_RANGE_RE = re.compile(r"^bytes=(\d*)-(\d*)$")


def parse_range(header: str, size: int) -> tuple:
    """Returns (start, end) inclusive. Raises 416 on invalid ranges."""
    unsatisfiable = HTTPException(
        status_code=416,
        detail="Requested range not satisfiable",
        headers={"Content-Range": f"bytes */{size}"},
    )
    # Only the first range is honoured (multi-range is rarely used by players)
    m = _RANGE_RE.match(header.strip().split(",")[0].strip())
    if not m:
        raise unsatisfiable
    s, e = m.groups()

    if s == "" and e == "":
        raise unsatisfiable
    if s == "":                      # suffix range: last N bytes
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


_END = object()


async def iter_byte_range(message, start: int, end: int) -> AsyncIterator[bytes]:
    """
    Pyrogram's stream_media uses CHUNK units (1 MiB) for offset/limit, not bytes.
    We fetch whole chunks covering [start, end] and trim the edges ourselves.

    The Telegram download runs in its own task and feeds a small queue
    (prefetch + backpressure). When the HTTP client goes away we only tell
    the task to stop after its current chunk. We never cancel it mid-request,
    because that corrupts Pyrogram's media connection.
    """
    first_chunk = start // TG_CHUNK
    last_chunk = end // TG_CHUNK
    skip = start - first_chunk * TG_CHUNK
    remaining = end - start + 1

    queue: asyncio.Queue = asyncio.Queue(maxsize=settings.prefetch_chunks)
    stop = asyncio.Event()
    failure: list = []

    async def producer() -> None:
        agen = bot.stream_media(
            message, limit=last_chunk - first_chunk + 1, offset=first_chunk
        )
        try:
            async for chunk in agen:
                if stop.is_set():
                    break
                await queue.put(chunk)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            failure.append(exc)
        finally:
            try:
                await agen.aclose()
            except Exception:
                pass
            await queue.put(_END)

    spawn(producer())

    try:
        while remaining > 0:
            try:
                item = await asyncio.wait_for(queue.get(), timeout=settings.stall_timeout)
            except asyncio.TimeoutError:
                raise TimeoutError(
                    f"No data from Telegram for {settings.stall_timeout}s"
                ) from None
            if item is _END:
                if failure:
                    raise failure[0]
                break
            chunk = item
            if skip:
                chunk = chunk[skip:]
                skip = 0
            if len(chunk) > remaining:
                chunk = chunk[:remaining]
            if chunk:
                remaining -= len(chunk)
                yield chunk
        if remaining > 0:
            logger.warning("Telegram returned %d bytes less than expected", remaining)
    finally:
        # Let the producer finish its current chunk and exit by itself.
        stop.set()
        while True:
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                break


class StreamSlot:
    """One unit of the concurrency budget. release() is idempotent."""

    __slots__ = ("_released",)

    def __init__(self) -> None:
        global active_streams
        active_streams += 1
        self._released = False

    def release(self) -> None:
        global active_streams
        if not self._released:
            self._released = True
            active_streams -= 1


class SlotStreamingResponse(StreamingResponse):
    """StreamingResponse that always frees its slot, even if the client
    disconnects before the body generator ever starts."""

    def __init__(self, *args, slot: StreamSlot, **kwargs):
        super().__init__(*args, **kwargs)
        self._slot = slot

    async def __call__(self, scope, receive, send):
        try:
            await super().__call__(scope, receive, send)
        finally:
            self._slot.release()


# --------------------------------------------------------------------------- #
# Routes
# --------------------------------------------------------------------------- #
@app.get("/")
async def root():
    return {"status": "Void MCU Streamer Bridge is Active", "version": app.version}


@app.get("/health")
async def health():
    connected = bool(bot.is_connected)
    sessions = getattr(bot, "media_sessions", None)
    body = {
        "status": "ok" if connected else "degraded",
        "telegram_connected": connected,
        "media_sessions": len(sessions) if isinstance(sessions, dict) else None,
        "active_streams": active_streams,
        "max_streams": settings.max_streams,
        "uptime_sec": int(time.time() - STARTED_AT),
    }
    return JSONResponse(body, status_code=200 if connected else 503)


@app.get("/info")
async def media_info(
    request: Request,
    message_id: int = Query(..., gt=0),
    token: Optional[str] = Query(None),
):
    check_auth(request, token)
    info = await resolve_media(message_id)
    return {
        "message_id": message_id,
        "name": info.name,
        "size": info.size,
        "mime": info.mime,
    }


@app.get("/test", include_in_schema=False)
async def test_page():
    """Serves void-test.html when it sits next to main.py (same origin = no CORS)."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "void-test.html")
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail="void-test.html not found next to main.py")
    return FileResponse(path, media_type="text/html")


@app.api_route("/stream", methods=["GET", "HEAD"])
async def stream_video(
    request: Request,
    message_id: int = Query(..., gt=0),
    token: Optional[str] = Query(None),
):
    check_auth(request, token)

    info = await resolve_media(message_id)

    # ---- Conditional request (cache validation) ---- #
    if request.headers.get("if-none-match") == info.etag:
        return Response(status_code=304, headers={"ETag": info.etag})

    # ---- Range handling ---- #
    range_header = request.headers.get("range")
    if range_header:
        if_range = request.headers.get("if-range")
        if if_range and if_range.strip() != info.etag:
            range_header = None  # validator changed: send the whole file
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
        "Cache-Control": "private, max-age=3600",
        "Content-Disposition": f"inline; filename*=UTF-8''{quote(info.name)}",
    }
    if status == 206:
        headers["Content-Range"] = f"bytes {start}-{end}/{info.size}"

    if request.method == "HEAD":
        return Response(status_code=status, headers=headers)

    # ---- Concurrency guard (check and take a slot with no await in between) ---- #
    if active_streams >= settings.max_streams:
        raise HTTPException(
            status_code=503, detail="Server busy, try again shortly",
            headers={"Retry-After": "5"},
        )
    slot = StreamSlot()

    async def body() -> AsyncIterator[bytes]:
        sent = 0
        try:
            async with aclosing(iter_byte_range(info.message, start, end)) as chunks:
                async for chunk in chunks:
                    sent += len(chunk)
                    yield chunk
        except asyncio.CancelledError:
            logger.info("Stream cancelled (msg %s) after %d bytes", message_id, sent)
            raise
        except FloodWait as fw:
            logger.warning("FloodWait %ss during stream (msg %s)", fw.value, message_id)
            cache.invalidate(message_id)
        except Exception as exc:
            # Headers are already sent; all we can do is log and end the stream
            # (the middleware then closes the connection so the player retries).
            if isinstance(exc, (TimeoutError, asyncio.TimeoutError)):
                logger.error(
                    "Stream stalled (msg %s) after %d/%d bytes: %s", message_id, sent, length, exc
                )
                spawn(reset_media_sessions(f"stall on msg {message_id}"))
            else:
                logger.exception("Stream failed (msg %s) after %d/%d bytes", message_id, sent, length)
            cache.invalidate(message_id)  # file_reference may have expired
        finally:
            slot.release()

    logger.info("Serving msg %s | %s | bytes %d-%d/%d", message_id, info.name, start, end, info.size)
    return SlotStreamingResponse(body(), status_code=status, headers=headers, slot=slot)


@app.exception_handler(Exception)
async def unhandled(_: Request, exc: Exception):
    # Log full details server-side, never leak internals to the client.
    logger.exception("Unhandled error: %s", exc)
    return JSONResponse({"detail": "Internal server error"}, status_code=500)
