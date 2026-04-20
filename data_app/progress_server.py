from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import asyncio
import json
import redis.asyncio as redis # Ensure you have 'redis' installed
from redis import Redis

app = FastAPI()

# Store active connections: {task_id: websocket}
connections = {}

# Redis configuration
REDIS_URL = "redis://localhost:6379/0"
PROGRESS_CHANNEL = "task_progress_updates"

@app.on_event("startup")
async def startup_event():
    """Start the background task that listens to Redis messages."""
    asyncio.create_task(redis_listener())

async def redis_listener():
    """Listens to Redis and routes messages to the correct WebSocket."""
    # Note the use of redis.from_url here
    r = await redis.from_url(REDIS_URL, decode_responses=True)
    pubsub = r.pubsub()
    await pubsub.subscribe(PROGRESS_CHANNEL)
    
    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                data = json.loads(message["data"])
                task_id = data.get("task_id")
                payload = data.get("payload")
                
                if task_id in connections:
                    ws = connections[task_id]
                    try:
                        await ws.send_json(payload)
                    except Exception:
                        connections.pop(task_id, None)
    except Exception as e:
        print(f"Redis Listener Error: {e}")
    finally:
        await pubsub.unsubscribe(PROGRESS_CHANNEL)

@app.websocket("/progress/{task_id}")
async def progress_socket(websocket: WebSocket, task_id: str):
    await websocket.accept()
    connections[task_id] = websocket
    print(f"📡 Client connected for task: {task_id}")
    try:
        # Keep connection open until client disconnects
        while True:
            await websocket.receive_text() 
    except WebSocketDisconnect:
        connections.pop(task_id, None)
        print(f"🔌 Client disconnected for task: {task_id}")

def push_progress(task_id: str, payload: dict):
    """
    Called by tasks.py. 
    Instead of sending directly to a dict (which it can't see), 
    it publishes to Redis.
    """
    r = Redis.from_url(REDIS_URL)
    message = {
        "task_id": task_id,
        "payload": payload
    }
    r.publish(PROGRESS_CHANNEL, json.dumps(message))