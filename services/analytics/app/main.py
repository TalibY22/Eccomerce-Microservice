import asyncio
import hashlib
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI, Request

TOPICS=["order.created","order.confirmed","order.rejected","fulfillment.events","delivery.events","payment.events","billing.events","notification.events","seller.events","support.events","warehouse.events"]
DDL="""CREATE TABLE IF NOT EXISTS domain_events (
 event_id String, event_type LowCardinality(String), topic LowCardinality(String), aggregate_id String,
 occurred_at DateTime64(3,'UTC'), ingested_at DateTime64(3,'UTC'), payload String
) ENGINE=ReplacingMergeTree(ingested_at) PARTITION BY toYYYYMM(occurred_at) ORDER BY (event_id)"""

class Store:
    def __init__(self):
        self.url=os.getenv("CLICKHOUSE_URL","http://analytics-db:8123"); self.database=os.getenv("CLICKHOUSE_DATABASE","analytics")
        self.user=os.getenv("CLICKHOUSE_USER","analytics"); self.password=os.getenv("CLICKHOUSE_PASSWORD","analyticspass")
    async def query(self,sql,data=None,params=None,database=None):
        async with httpx.AsyncClient(timeout=15) as client:
            resp=await client.post(self.url,params={"database":database or self.database,"query":sql,**(params or {})},content=data,auth=(self.user,self.password)); resp.raise_for_status(); return resp.text
    async def init(self): await self.query("CREATE DATABASE IF NOT EXISTS analytics",database="default"); await self.query(DDL)
    async def insert(self, rows):
        if not rows:return
        body="\n".join(json.dumps(row,separators=(",",":"),ensure_ascii=False) for row in rows)+"\n"
        await self.query("INSERT INTO domain_events FORMAT JSONEachRow",body.encode())
    async def json_rows(self,sql):
        text=await self.query(sql+" FORMAT JSONEachRow")
        return [json.loads(line) for line in text.splitlines() if line]

async def consume(app):
    consumer=AIOKafkaConsumer(*TOPICS,bootstrap_servers=os.getenv("KAFKA_BROKER","localhost:9094"),group_id="analytics-v1",enable_auto_commit=False,auto_offset_reset="earliest")
    await consumer.start()
    try:
        async for msg in consumer:
            try:
                event=json.loads(msg.value)
                if not isinstance(event,dict): raise ValueError("not an event object")
                event_id=event.get("event_id") or hashlib.sha256(f"{msg.topic}:{msg.partition}:{msg.offset}".encode()).hexdigest()
                event_type=str(event.get("event_type",msg.topic))
                aggregate=str(event.get("order_id") or event.get("shipment_id") or event.get("invoice_id") or event.get("user_id") or "")
                raw_time=event.get("occurred_at") or event.get("created_at")
                try: dt=datetime.fromisoformat(str(raw_time).replace("Z","+00:00")) if raw_time else datetime.now(timezone.utc)
                except ValueError: dt=datetime.now(timezone.utc)
                if dt.tzinfo is None: dt=dt.replace(tzinfo=timezone.utc)
                moment=dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                await app.state.store.insert([{"event_id":str(event_id),"event_type":event_type,"topic":msg.topic,"aggregate_id":aggregate,"occurred_at":moment,"ingested_at":datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],"payload":json.dumps(event,separators=(",",":"),ensure_ascii=False)}])
                await consumer.commit()
            except asyncio.CancelledError: raise
            except Exception as e:
                if isinstance(e,ValueError):
                    raw=json.dumps({"error":str(e),"raw":msg.value.decode(errors="replace")}).encode()
                    await app.state.producer.send_and_wait(msg.topic+".dlq",raw,key=msg.key); await consumer.commit()
                else:
                    import logging; logging.exception("analytics event ingest failed"); await asyncio.sleep(2)
    finally: await consumer.stop()

@asynccontextmanager
async def lifespan(app):
    app.state.store=Store(); await app.state.store.init()
    from aiokafka import AIOKafkaProducer
    app.state.producer=AIOKafkaProducer(bootstrap_servers=os.getenv("KAFKA_BROKER","localhost:9094")); await app.state.producer.start()
    task=asyncio.create_task(consume(app))
    try: yield
    finally:
        task.cancel(); await asyncio.gather(task,return_exceptions=True); await app.state.producer.stop()

app=FastAPI(title="Analytics Service",lifespan=lifespan)
@app.get("/health")
def health(): return {"status":"ok"}
@app.get("/events/count")
async def count(request:Request):
    rows=await request.app.state.store.json_rows("SELECT count() AS count FROM domain_events FINAL")
    return rows[0] if rows else {"count":0}
@app.get("/sales")
async def sales(request:Request):
    return await request.app.state.store.json_rows("SELECT JSONExtractString(payload,'currency') AS currency, countIf(event_type IN ('payment.captured','payment.succeeded')) AS payments, sumIf(toInt64OrZero(JSONExtractString(payload,'amount_minor')), event_type IN ('payment.captured','payment.succeeded')) AS gross_minor FROM domain_events FINAL GROUP BY currency ORDER BY currency")
@app.get("/events/recent")
async def recent(request:Request,limit:int=100):
    limit=max(1,min(limit,500))
    return await request.app.state.store.json_rows(f"SELECT event_id,event_type,topic,aggregate_id,occurred_at,payload FROM domain_events FINAL ORDER BY occurred_at DESC LIMIT {limit}")
