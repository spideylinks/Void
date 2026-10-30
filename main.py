import os
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
from pyrogram import Client

app = FastAPI()

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = int(os.environ.get("CHAT_ID", 0))

# Initialize Telegram Bot Client
bot = Client(
    "mcu_streamer",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True
)

@app.on_event("startup")
async def startup_event():
    await bot.start()

@app.on_event("shutdown")
async def shutdown_event():
    await bot.stop()

@app.get("/")
async def root():
    return {"status": "MCU Streamer Bridge is Active!"}

@app.get("/stream")
async def stream_video(request: Request, message_id: int):
    try:
        # Telegram group se message fetch karo
        message = await bot.get_messages(CHAT_ID, message_id)
        if not message or not (message.video or message.document):
            raise HTTPException(status_code=404, detail="Movie not found in Telegram!")
        
        media = message.video or message.document
        file_size = media.file_size
        file_id = media.file_id

        # Video scrubbing (aage-peeche karne) ke liye Range Header handle karna
        range_header = request.headers.get("range")
        start = 0
        end = file_size - 1

        if range_header:
            bytes_range = range_header.replace("bytes=", "").split("-")
            start = int(bytes_range[0])
            if len(bytes_range) > 1 and bytes_range[1]:
                end = int(bytes_range[1])

        length = end - start + 1

        async def file_streamer():
            # Telegram se chunk-by-chunk data stream karna
            async for chunk in bot.stream_media(file_id, offset=start, limit=length):
                yield chunk

        headers = {
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Accept-Ranges": "bytes",
            "Content-Length": str(length),
            "Content-Type": getattr(media, "mime_type", "video/mp4"),
        }

        return StreamingResponse(file_streamer(), status_code=206, headers=headers)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
