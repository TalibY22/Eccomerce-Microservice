import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from shared.runtime import event_runtime
from .handlers import handle_event, send_pending


@asynccontextmanager
async def lifespan(app):
    async with event_runtime(app, "notification", ["order.created", "order.confirmed", "order.rejected", "payment.events", "delivery.events", "seller.events", "support.events", "billing.events"], handle_event, "/app/migrations"):
        task = asyncio.create_task(_sender(app))
        try: yield
        finally: task.cancel(); await asyncio.gather(task, return_exceptions=True)


async def _sender(app):
    while True:
        try: await send_pending(app.state.db, app.state.producer)
        except asyncio.CancelledError: raise
        except Exception: import logging; logging.exception("notification sender failed")
        await asyncio.sleep(2)


app = FastAPI(title="Notification Service", lifespan=lifespan)


class Contact(BaseModel):
    user_id: str
    channel: str
    destination: str
    verified: bool = False

class Preference(BaseModel):
    enabled: bool = True

class Template(BaseModel):
    event_type: str
    channel: str
    body: str
    subject: str | None = None


@app.get("/health")
def health(): return {"status": "ok"}

@app.put("/contacts")
async def set_contact(value: Contact, request: Request):
    if value.channel not in {"email", "sms", "push"}: raise HTTPException(422, "channel must be email, sms, or push")
    async with request.app.state.db.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("INSERT INTO notification_contacts(user_id,channel,destination,verified) VALUES(%s,%s,%s,%s) ON DUPLICATE KEY UPDATE destination=VALUES(destination),verified=VALUES(verified)", (value.user_id,value.channel,value.destination,value.verified)); await conn.commit()
    return {"status":"saved"}

@app.put("/preferences/{user_id}/{channel}")
async def set_preference(user_id: str, channel: str, value: Preference, request: Request):
    async with request.app.state.db.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("INSERT INTO notification_preferences(user_id,channel,enabled) VALUES(%s,%s,%s) ON DUPLICATE KEY UPDATE enabled=VALUES(enabled)", (user_id,channel,value.enabled)); await conn.commit()
    return {"user_id":user_id,"channel":channel,"enabled":value.enabled}

@app.put("/templates")
async def set_template(value: Template, request: Request):
    async with request.app.state.db.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("INSERT INTO notification_templates(event_type,channel,subject_template,body_template) VALUES(%s,%s,%s,%s) ON DUPLICATE KEY UPDATE subject_template=VALUES(subject_template),body_template=VALUES(body_template)", (value.event_type,value.channel,value.subject,value.body)); await conn.commit()
    return {"status":"saved"}

@app.get("/attempts/{attempt_id}")
async def get_attempt(attempt_id: str, request: Request):
    async with request.app.state.db.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT id,event_id,user_id,event_type,channel,destination,status,provider_message_id,error,attempts,created_at FROM notification_attempts WHERE id=%s", (attempt_id,)); row=await cur.fetchone()
    if not row: raise HTTPException(404,"attempt not found")
    return dict(zip(["id","event_id","user_id","event_type","channel","destination","status","provider_message_id","error","attempts","created_at"],row))
