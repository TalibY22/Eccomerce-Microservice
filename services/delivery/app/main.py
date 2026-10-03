import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request, Header
from pydantic import BaseModel

from shared.runtime import enqueue_event, event_runtime
from .handlers import handle_event


@asynccontextmanager
async def lifespan(app):
    async with event_runtime(app,"delivery",["order.created","order.confirmed","fulfillment.events"],handle_event,"/app/migrations"):
        yield

app=FastAPI(title="Delivery Service",lifespan=lifespan)

class Address(BaseModel):
    address: dict
    carrier: str | None = None
    tracking_number: str | None = None

class CarrierUpdate(BaseModel):
    carrier_event_id: str
    status: str
    description: str | None = None
    location: str | None = None
    occurred_at: datetime | None = None

@app.get("/health")
def health(): return {"status":"ok"}

@app.get("/shipments/{order_id}")
async def shipment(order_id: str, request: Request):
    async with request.app.state.db.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT id,order_id,user_id,carrier,tracking_number,status,shipping_address,created_at,updated_at FROM shipments WHERE order_id=%s", (order_id,)); row=await cur.fetchone()
            if not row: raise HTTPException(404,"shipment not found")
            await cur.execute("SELECT event_key,status,description,location,occurred_at FROM delivery_timeline WHERE shipment_id=%s ORDER BY occurred_at,id",(row[0],)); timeline=await cur.fetchall()
    names=["id","order_id","user_id","carrier","tracking_number","status","shipping_address","created_at","updated_at"]
    result=dict(zip(names,row));
    if isinstance(result["shipping_address"],(bytes,str)):
        try: result["shipping_address"]=json.loads(result["shipping_address"])
        except Exception: pass
    result["timeline"]=[dict(zip(["event_key","status","description","location","occurred_at"],entry)) for entry in timeline]
    return result

@app.put("/shipments/{order_id}/address")
async def set_address(order_id: str, value: Address, request: Request):
    async with request.app.state.db.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT id FROM shipments WHERE order_id=%s FOR UPDATE",(order_id,)); row=await cur.fetchone()
            if not row: raise HTTPException(404,"shipment not found")
            await cur.execute("UPDATE shipments SET shipping_address=%s,carrier=%s,tracking_number=%s,status='label_created' WHERE id=%s",(json.dumps(value.address),value.carrier,value.tracking_number,row[0]))
            await _record(cur,row[0],"address:"+str(uuid4()),"label_created","Shipping address saved",None)
            await enqueue_event(cur,"delivery.events",order_id,{"event_type":"delivery.status.updated","shipment_id":row[0],"order_id":order_id,"status":"label_created","carrier":value.carrier,"tracking_number":value.tracking_number})
        await conn.commit()
    return {"order_id":order_id,"status":"label_created"}

@app.post("/webhooks/carriers/{order_id}")
async def carrier_update(order_id: str, value: CarrierUpdate, request: Request, x_webhook_secret: str | None=Header(default=None)):
    import os
    secret=os.getenv("CARRIER_WEBHOOK_SECRET")
    if secret and x_webhook_secret != secret: raise HTTPException(401,"invalid webhook secret")
    async with request.app.state.db.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT id,user_id FROM shipments WHERE order_id=%s FOR UPDATE",(order_id,)); row=await cur.fetchone()
            if not row: raise HTTPException(404,"shipment not found")
            occurred=value.occurred_at or datetime.now(timezone.utc)
            if occurred.tzinfo is None: occurred=occurred.replace(tzinfo=timezone.utc)
            else: occurred=occurred.astimezone(timezone.utc)
            await cur.execute("INSERT IGNORE INTO delivery_timeline(shipment_id,event_key,status,description,location,occurred_at) VALUES(%s,%s,%s,%s,%s,%s)",(row[0],value.carrier_event_id,value.status,value.description,value.location,occurred.replace(tzinfo=None)))
            if cur.rowcount:
                await cur.execute("UPDATE shipments SET status=%s WHERE id=%s",(value.status,row[0]))
                await enqueue_event(cur,"delivery.events",order_id,{"event_type":"delivery.status.updated","shipment_id":row[0],"order_id":order_id,"user_id":row[1],"status":value.status,"description":value.description,"location":value.location,"occurred_at":occurred.isoformat()})
        await conn.commit()
    return {"accepted":True,"duplicate":not bool(cur.rowcount)}

async def _record(cur,shipment_id,event_key,status,description,location):
    await cur.execute("INSERT IGNORE INTO delivery_timeline(shipment_id,event_key,status,description,location) VALUES(%s,%s,%s,%s,%s)",(shipment_id,event_key,status,description,location))
