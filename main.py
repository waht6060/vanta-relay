import asyncio
import uuid
import os
from fastapi import FastAPI, Request, Response, HTTPException

AUTH_TOKEN = os.environ.get("AUTH_TOKEN", "vanta-secret-pass-2026")

app = FastAPI()
sessions = {}

@app.get("/")
async def health_check():
    return {"status": "online"}

@app.post("/vanta/open")
async def tunnel_open(req: Request):
    data = await req.json()
    if data.get("auth") != AUTH_TOKEN:
        raise HTTPException(status_code=403, detail="Unauthorized")
    
    target = data.get("target")
    host, port = target.split(":")
    port = int(port)

    try:
        reader, writer = await asyncio.open_connection(host, port)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Target unreachable: {e}")

    session_id = str(uuid.uuid4())
    inbound_queue = asyncio.Queue()

    async def socket_reader():
        try:
            while True:
                buf = await reader.read(65536)
                if not buf:
                    break
                await inbound_queue.put(buf)
        except Exception:
            pass
        finally:
            await inbound_queue.put(None)

    reader_task = asyncio.create_task(socket_reader())

    sessions[session_id] = {
        "writer": writer,
        "queue": inbound_queue,
        "task": reader_task,
        "active": True
    }

    return {"status": "ok", "session_id": session_id}

@app.post("/vanta/push")
async def tunnel_push(req: Request):
    session_id = req.headers.get("X-Session-ID")
    sess = sessions.get(session_id)
    if not sess or not sess["active"]:
        raise HTTPException(status_code=404, detail="Session expired")

    body = await req.body()
    if body:
        try:
            sess["writer"].write(body)
            await sess["writer"].drain()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    return {"status": "ok"}

@app.get("/vanta/pull")
async def tunnel_pull(req: Request):
    session_id = req.headers.get("X-Session-ID")
    sess = sessions.get(session_id)
    if not sess or not sess["active"]:
        raise HTTPException(status_code=404, detail="Session expired")

    queue = sess["queue"]
    try:
        data = await asyncio.wait_for(queue.get(), timeout=10.0)
        if data is None:
            sess["active"] = False
            return Response(content=b"", media_type="application/octet-stream", headers={"X-EOF": "1"})
        return Response(content=data, media_type="application/octet-stream")
    except asyncio.TimeoutError:
        return Response(content=b"", media_type="application/octet-stream")

@app.post("/vanta/close")
async def tunnel_close(req: Request):
    session_id = req.headers.get("X-Session-ID")
    sess = sessions.pop(session_id, None)
    if sess:
        sess["active"] = False
        sess["task"].cancel()
        try:
            sess["writer"].close()
            await sess["writer"].wait_closed()
        except Exception:
            pass
    return {"status": "closed"}
