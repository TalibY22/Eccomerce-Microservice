import asyncio
import json
import logging
from datetime import datetime, timezone
import os
from contextlib import asynccontextmanager
from uuid import uuid4

import aiomysql
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from app.database import apply_migrations, import_legacy_product_stock
from app.consumer import process_order_created

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("warehouse")


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await aiomysql.create_pool(
        host=os.getenv("DB_HOST", "localhost"), port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "warehouse_svc"), password=os.getenv("DB_PASSWORD", "warehousepass"),
        db=os.getenv("DB_NAME", "warehouse_service"), autocommit=False, minsize=1, maxsize=10,
        charset="utf8mb4",
    )
    await apply_migrations(pool)
    await import_legacy_product_stock(pool)
    app.state.db = pool
    broker = os.getenv("KAFKA_BROKER", "localhost:9094")
    producer = AIOKafkaProducer(bootstrap_servers=broker, enable_idempotence=True)
    consumer = AIOKafkaConsumer(
        "order.created", bootstrap_servers=broker, group_id="warehouse-v1",
        enable_auto_commit=False, auto_offset_reset="earliest",
    )
    app.state.producer = producer
    await producer.start()
    await consumer.start()
    tasks = [asyncio.create_task(consume_orders(app, consumer)), asyncio.create_task(publish_outbox(app))]
    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await consumer.stop()
        await producer.stop()
        pool.close()
        await pool.wait_closed()


app = FastAPI(title="Warehouse Service", version="1.0.0", lifespan=lifespan)


class WarehouseInput(BaseModel):
    code: str = Field(min_length=2, max_length=40)
    name: str = Field(min_length=2, max_length=160)


class AdjustmentInput(BaseModel):
    product_id: str
    sku: str = Field(min_length=1, max_length=64)
    quantity_delta: int
    reorder_level: int = Field(default=0, ge=0)
    reason: str = Field(min_length=2, max_length=255)


class ReservationItem(BaseModel):
    product_id: str
    quantity: int = Field(gt=0)


class ReservationInput(BaseModel):
    order_id: str
    items: list[ReservationItem] = Field(min_length=1)


class TransferItem(BaseModel):
    product_id: str
    sku: str
    quantity: int = Field(gt=0)


class TransferInput(BaseModel):
    source_warehouse_id: str
    destination_warehouse_id: str
    items: list[TransferItem] = Field(min_length=1)


@app.get("/health")
async def health(request: Request):
    async with request.app.state.db.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1")
            await cur.fetchone()
    return {"status": "ok"}


@app.post("/warehouses", status_code=201)
async def create_warehouse(body: WarehouseInput, request: Request):
    warehouse_id = str(uuid4())
    async with request.app.state.db.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("INSERT INTO warehouses(id, code, name) VALUES(%s,%s,%s)", (warehouse_id, body.code, body.name))
        await conn.commit()
    return {"id": warehouse_id, "code": body.code, "name": body.name}


@app.post("/warehouses/{warehouse_id}/adjustments", status_code=201)
async def adjust_stock(warehouse_id: str, body: AdjustmentInput, request: Request):
    if body.quantity_delta == 0:
        raise HTTPException(422, "quantity_delta must be nonzero")
    pool = request.app.state.db
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await conn.begin()
            await cur.execute("INSERT INTO inventory_balances(warehouse_id,product_id,sku) VALUES(%s,%s,%s) ON DUPLICATE KEY UPDATE sku=VALUES(sku)", (warehouse_id, body.product_id, body.sku))
            await cur.execute("SELECT quantity_on_hand, quantity_reserved FROM inventory_balances WHERE warehouse_id=%s AND product_id=%s FOR UPDATE", (warehouse_id, body.product_id))
            row = await cur.fetchone()
            updated = row["quantity_on_hand"] + body.quantity_delta
            if updated < row["quantity_reserved"]:
                await conn.rollback()
                raise HTTPException(409, "adjustment would reduce stock below reserved quantity")
            await cur.execute("UPDATE inventory_balances SET sku=%s, quantity_on_hand=%s, reorder_level=%s WHERE warehouse_id=%s AND product_id=%s", (body.sku, updated, body.reorder_level, warehouse_id, body.product_id))
            await cur.execute("INSERT INTO stock_movements(warehouse_id,product_id,sku,movement_type,quantity_delta,reason) VALUES(%s,%s,%s,'adjustment',%s,%s)", (warehouse_id, body.product_id, body.sku, body.quantity_delta, body.reason))
            previous_available = row["quantity_on_hand"] - row["quantity_reserved"]
            await enqueue_low_stock(cur, warehouse_id, body.product_id, body.sku, previous_available, updated-row["quantity_reserved"], body.reorder_level)
            await enqueue_event(cur, "inventory.events", body.product_id, {"event_id":str(uuid4()),"event_type":"inventory.stock_adjusted","schema_version":1,"warehouse_id":warehouse_id,"product_id":body.product_id,"sku":body.sku,"quantity_delta":body.quantity_delta,"on_hand":updated,"reason":body.reason,"occurred_at":datetime.now(timezone.utc).isoformat()})
        await conn.commit()
    return {"warehouse_id": warehouse_id, "product_id": body.product_id, "on_hand": updated, "available": updated-row["quantity_reserved"]}


@app.get("/warehouses/{warehouse_id}/inventory")
async def warehouse_inventory(warehouse_id: str, request: Request):
    async with request.app.state.db.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT product_id,sku,quantity_on_hand,quantity_reserved,quantity_on_hand-quantity_reserved AS available,reorder_level FROM inventory_balances WHERE warehouse_id=%s ORDER BY sku", (warehouse_id,))
            return await cur.fetchall()


@app.post("/reservations", status_code=201)
async def reserve_stock(body: ReservationInput, request: Request):
    requested = {}
    for item in body.items:
        requested[item.product_id] = requested.get(item.product_id, 0) + item.quantity
    async with request.app.state.db.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await conn.begin()
            await cur.execute("SELECT state FROM order_reservations WHERE order_id=%s FOR UPDATE", (body.order_id,))
            existing = await cur.fetchone()
            if existing:
                await conn.rollback()
                if existing["state"] == "reserved":
                    return {"order_id": body.order_id, "state": "reserved", "idempotent": True}
                raise HTTPException(409, "reservation already exists in a terminal state")
            allocations = []
            for product_id, wanted in sorted(requested.items()):
                await cur.execute("SELECT warehouse_id,sku,quantity_on_hand,quantity_reserved,reorder_level FROM inventory_balances WHERE product_id=%s ORDER BY warehouse_id FOR UPDATE", (product_id,))
                rows = await cur.fetchall()
                if sum(row["quantity_on_hand"]-row["quantity_reserved"] for row in rows) < wanted:
                    await conn.rollback()
                    raise HTTPException(409, f"insufficient stock for {product_id}")
                remaining = wanted
                for row in rows:
                    take = min(remaining,row["quantity_on_hand"]-row["quantity_reserved"])
                    if take:
                        allocations.append((product_id,row,take))
                        remaining -= take
                    if remaining == 0: break
            await cur.execute("INSERT INTO order_reservations(order_id,state) VALUES(%s,'reserved')", (body.order_id,))
            for product_id,row,quantity in allocations:
                await cur.execute("UPDATE inventory_balances SET quantity_reserved=quantity_reserved+%s WHERE warehouse_id=%s AND product_id=%s", (quantity,row["warehouse_id"],product_id))
                await cur.execute("INSERT INTO order_reservation_items(order_id,warehouse_id,product_id,sku,quantity) VALUES(%s,%s,%s,%s,%s)", (body.order_id,row["warehouse_id"],product_id,row["sku"],quantity))
                await cur.execute("INSERT INTO stock_movements(warehouse_id,product_id,sku,movement_type,quantity_delta,reserved_delta,reference_type,reference_id) VALUES(%s,%s,%s,'reserved',0,%s,'order',%s)", (row["warehouse_id"],product_id,row["sku"],quantity,body.order_id))
                previous_available = row["quantity_on_hand"]-row["quantity_reserved"]
                await enqueue_low_stock(cur,row["warehouse_id"],product_id,row["sku"],previous_available,previous_available-quantity,row["reorder_level"])
        await conn.commit()
    return {"order_id": body.order_id, "state": "reserved", "allocations": [{"warehouse_id":r["warehouse_id"],"product_id":pid,"quantity":q} for pid,r,q in allocations]}


@app.post("/reservations/{order_id}/commit")
async def commit_reservation(order_id: str, request: Request):
    async with request.app.state.db.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await conn.begin()
            await cur.execute("SELECT state FROM order_reservations WHERE order_id=%s FOR UPDATE", (order_id,))
            reservation = await cur.fetchone()
            if not reservation:
                await conn.rollback(); raise HTTPException(404, "reservation not found")
            if reservation["state"] == "committed":
                await conn.commit(); return {"order_id": order_id, "state": "committed"}
            if reservation["state"] != "reserved":
                await conn.rollback(); raise HTTPException(409, "reservation is not commit-ready")
            await cur.execute("SELECT warehouse_id,product_id,sku,quantity FROM order_reservation_items WHERE order_id=%s ORDER BY warehouse_id,product_id", (order_id,))
            items = await cur.fetchall()
            for item in items:
                await cur.execute("UPDATE inventory_balances SET quantity_on_hand=quantity_on_hand-%s,quantity_reserved=quantity_reserved-%s WHERE warehouse_id=%s AND product_id=%s", (item["quantity"],item["quantity"],item["warehouse_id"],item["product_id"]))
                await cur.execute("INSERT INTO stock_movements(warehouse_id,product_id,sku,movement_type,quantity_delta,reserved_delta,reference_type,reference_id) VALUES(%s,%s,%s,'reservation_committed',%s,%s,'order',%s)", (item["warehouse_id"],item["product_id"],item["sku"],-item["quantity"],-item["quantity"],order_id))
            await cur.execute("UPDATE order_reservations SET state='committed' WHERE order_id=%s", (order_id,))
        await conn.commit()
    return {"order_id": order_id, "state": "committed"}


@app.post("/reservations/{order_id}/release")
async def release_reservation(order_id: str, request: Request):
    async with request.app.state.db.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await conn.begin()
            await cur.execute("SELECT state FROM order_reservations WHERE order_id=%s FOR UPDATE", (order_id,))
            reservation = await cur.fetchone()
            if not reservation:
                await conn.rollback()
                raise HTTPException(404, "reservation not found")
            if reservation["state"] == "released":
                await conn.commit()
                return {"order_id": order_id, "state": "released"}
            if reservation["state"] not in {"reserved", "committed"}:
                await conn.rollback()
                raise HTTPException(409, "reservation cannot be released in its current state")
            was_committed = reservation["state"] == "committed"
            await cur.execute("SELECT warehouse_id,product_id,sku,quantity FROM order_reservation_items WHERE order_id=%s ORDER BY warehouse_id,product_id", (order_id,))
            items = await cur.fetchall()
            for item in items:
                if was_committed:
                    await cur.execute("UPDATE inventory_balances SET quantity_on_hand=quantity_on_hand+%s WHERE warehouse_id=%s AND product_id=%s", (item["quantity"],item["warehouse_id"],item["product_id"]))
                    delta = item["quantity"]
                else:
                    await cur.execute("UPDATE inventory_balances SET quantity_reserved=quantity_reserved-%s WHERE warehouse_id=%s AND product_id=%s", (item["quantity"],item["warehouse_id"],item["product_id"]))
                    delta = 0
                await cur.execute("INSERT INTO stock_movements(warehouse_id,product_id,sku,movement_type,quantity_delta,reserved_delta,reference_type,reference_id) VALUES(%s,%s,%s,'order_released',%s,%s,'order',%s)", (item["warehouse_id"],item["product_id"],item["sku"],delta,-item["quantity"],order_id))
            await cur.execute("UPDATE order_reservations SET state='released' WHERE order_id=%s", (order_id,))
        await conn.commit()
    return {"order_id": order_id, "state": "released"}


@app.get("/warehouses")
async def list_warehouses(request: Request):
    async with request.app.state.db.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT id,code,name,active,created_at FROM warehouses ORDER BY name")
            return await cur.fetchall()


@app.post("/transfers/{transfer_id}/cancel")
async def cancel_transfer(transfer_id: str, request: Request):
    async with request.app.state.db.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await conn.begin()
            await cur.execute("SELECT source_warehouse_id,status FROM stock_transfers WHERE id=%s FOR UPDATE", (transfer_id,))
            transfer = await cur.fetchone()
            if not transfer:
                await conn.rollback(); raise HTTPException(404, "transfer not found")
            if transfer["status"] == "cancelled":
                await conn.commit(); return {"id": transfer_id, "status": "cancelled"}
            if transfer["status"] != "in_transit":
                await conn.rollback(); raise HTTPException(409, "only an in-transit transfer can be cancelled")
            await cur.execute("SELECT product_id,sku,quantity FROM stock_transfer_items WHERE transfer_id=%s", (transfer_id,))
            items = await cur.fetchall()
            for item in items:
                await cur.execute("UPDATE inventory_balances SET quantity_on_hand=quantity_on_hand+%s WHERE warehouse_id=%s AND product_id=%s", (item["quantity"],transfer["source_warehouse_id"],item["product_id"]))
                await cur.execute("INSERT INTO stock_movements(warehouse_id,product_id,sku,movement_type,quantity_delta,reference_type,reference_id) VALUES(%s,%s,%s,'transfer_cancelled',%s,'transfer',%s)", (transfer["source_warehouse_id"],item["product_id"],item["sku"],item["quantity"],transfer_id))
            await cur.execute("UPDATE stock_transfers SET status='cancelled' WHERE id=%s", (transfer_id,))
            await enqueue_event(cur, "inventory.events", transfer_id, {"event_id":str(uuid4()),"event_type":"inventory.transfer_cancelled","schema_version":1,"transfer_id":transfer_id,"occurred_at":datetime.now(timezone.utc).isoformat()})
        await conn.commit()
    return {"id": transfer_id, "status": "cancelled"}


@app.get("/alerts/low-stock")
async def low_stock(request: Request):
    async with request.app.state.db.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("""SELECT b.warehouse_id,w.code AS warehouse_code,b.product_id,b.sku,
                b.quantity_on_hand-b.quantity_reserved AS available,b.reorder_level
                FROM inventory_balances b JOIN warehouses w ON w.id=b.warehouse_id
                WHERE b.quantity_on_hand-b.quantity_reserved <= b.reorder_level ORDER BY w.code,b.sku""")
            return await cur.fetchall()


@app.get("/movements")
async def list_movements(request: Request, product_id: str | None = None, limit: int = 100):
    limit = max(1, min(limit, 500))
    async with request.app.state.db.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            if product_id:
                await cur.execute(f"SELECT * FROM stock_movements WHERE product_id=%s ORDER BY id DESC LIMIT {limit}", (product_id,))
            else:
                await cur.execute(f"SELECT * FROM stock_movements ORDER BY id DESC LIMIT {limit}")
            return await cur.fetchall()


@app.post("/transfers", status_code=201)
async def create_transfer(body: TransferInput, request: Request):
    if body.source_warehouse_id == body.destination_warehouse_id:
        raise HTTPException(400, "source and destination warehouses must differ")
    if len({item.product_id for item in body.items}) != len(body.items):
        raise HTTPException(422, "transfer items must have unique product ids")
    transfer_id = str(uuid4())
    pool = request.app.state.db
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await conn.begin()
            try:
                source_rows = {}
                for item in sorted(body.items, key=lambda i: i.product_id):
                    await cur.execute("SELECT quantity_on_hand,quantity_reserved,reorder_level FROM inventory_balances WHERE warehouse_id=%s AND product_id=%s FOR UPDATE", (body.source_warehouse_id,item.product_id))
                    row = await cur.fetchone()
                    if not row or row["quantity_on_hand"]-row["quantity_reserved"] < item.quantity:
                        raise HTTPException(409, f"insufficient source stock for {item.product_id}")
                    source_rows[item.product_id] = row
                await cur.execute("INSERT INTO stock_transfers(id,source_warehouse_id,destination_warehouse_id,status) VALUES(%s,%s,%s,'in_transit')", (transfer_id,body.source_warehouse_id,body.destination_warehouse_id))
                for item in body.items:
                    await cur.execute("UPDATE inventory_balances SET quantity_on_hand=quantity_on_hand-%s WHERE warehouse_id=%s AND product_id=%s", (item.quantity,body.source_warehouse_id,item.product_id))
                    await cur.execute("INSERT INTO stock_transfer_items(transfer_id,product_id,sku,quantity) VALUES(%s,%s,%s,%s)", (transfer_id,item.product_id,item.sku,item.quantity))
                    await cur.execute("INSERT INTO stock_movements(warehouse_id,product_id,sku,movement_type,quantity_delta,reference_type,reference_id) VALUES(%s,%s,%s,'transfer_out',%s,'transfer',%s)", (body.source_warehouse_id,item.product_id,item.sku,-item.quantity,transfer_id))
                    source_row = source_rows[item.product_id]
                    await enqueue_low_stock(cur,body.source_warehouse_id,item.product_id,item.sku,source_row["quantity_on_hand"]-source_row["quantity_reserved"],source_row["quantity_on_hand"]-source_row["quantity_reserved"]-item.quantity,source_row["reorder_level"])
                await enqueue_event(cur, "inventory.events", transfer_id, {"event_id":str(uuid4()),"event_type":"inventory.transfer_dispatched","schema_version":1,"transfer_id":transfer_id,"source_warehouse_id":body.source_warehouse_id,"destination_warehouse_id":body.destination_warehouse_id,"items":[item.model_dump() for item in body.items],"occurred_at":datetime.now(timezone.utc).isoformat()})
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise
    return {"id": transfer_id, "status": "in_transit"}


@app.post("/transfers/{transfer_id}/receive")
async def receive_transfer(transfer_id: str, request: Request):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await conn.begin()
            await cur.execute("SELECT source_warehouse_id,destination_warehouse_id,status FROM stock_transfers WHERE id=%s FOR UPDATE", (transfer_id,))
            transfer = await cur.fetchone()
            if not transfer:
                await conn.rollback(); raise HTTPException(404, "transfer not found")
            if transfer["status"] == "received":
                await conn.commit(); return {"id": transfer_id, "status": "received"}
            if transfer["status"] != "in_transit":
                await conn.rollback(); raise HTTPException(409, "transfer is not in transit")
            await cur.execute("SELECT product_id,sku,quantity FROM stock_transfer_items WHERE transfer_id=%s", (transfer_id,))
            items = await cur.fetchall()
            for item in items:
                await cur.execute("INSERT INTO inventory_balances(warehouse_id,product_id,sku) VALUES(%s,%s,%s) ON DUPLICATE KEY UPDATE sku=VALUES(sku)", (transfer["destination_warehouse_id"],item["product_id"],item["sku"]))
                await cur.execute("UPDATE inventory_balances SET quantity_on_hand=quantity_on_hand+%s WHERE warehouse_id=%s AND product_id=%s", (item["quantity"],transfer["destination_warehouse_id"],item["product_id"]))
                await cur.execute("INSERT INTO stock_movements(warehouse_id,product_id,sku,movement_type,quantity_delta,reference_type,reference_id) VALUES(%s,%s,%s,'transfer_in',%s,'transfer',%s)", (transfer["destination_warehouse_id"],item["product_id"],item["sku"],item["quantity"],transfer_id))
            await cur.execute("UPDATE stock_transfers SET status='received',received_at=CURRENT_TIMESTAMP(6) WHERE id=%s", (transfer_id,))
            await enqueue_event(cur, "inventory.events", transfer_id, {"event_id":str(uuid4()),"event_type":"inventory.transfer_received","schema_version":1,"transfer_id":transfer_id,"occurred_at":datetime.now(timezone.utc).isoformat()})
        await conn.commit()
    return {"id": transfer_id, "status": "received"}


async def enqueue_event(cur, topic, key, event):
    payload = json.dumps(event, separators=(",", ":"))
    await cur.execute("INSERT INTO outbox_events(id,topic,message_key,payload) VALUES(%s,%s,%s,%s)", (event["event_id"],topic,key,payload))


async def enqueue_low_stock(cur, warehouse_id, product_id, sku, previous_available, available, reorder_level):
    if previous_available > reorder_level >= available:
        event_id = str(uuid4())
        payload = json.dumps({"event_id": event_id, "event_type": "inventory.low_stock", "warehouse_id": warehouse_id, "product_id": product_id, "sku": sku, "available": available, "reorder_level": reorder_level})
        await cur.execute("INSERT INTO outbox_events(id,topic,message_key,payload) VALUES(%s,'inventory.events',%s,%s)", (event_id,product_id,payload))


async def consume_orders(app: FastAPI, consumer: AIOKafkaConsumer):
    async for msg in consumer:
        while True:
            try:
                event = json.loads(msg.value)
                await process_order_created(app.state.db, event)
                await consumer.commit()
                break
            except (json.JSONDecodeError, ValueError, TypeError) as exc:
                log.error("invalid order.created offset=%s: %s", msg.offset, exc)
                raw_message = msg.value.decode("utf-8", errors="replace") if isinstance(msg.value, bytes) else repr(msg.value)
                payload = json.dumps({"error": str(exc), "raw": raw_message}).encode()
                try:
                    await app.state.producer.send_and_wait("order.created.dlq", payload, key=msg.key)
                    await consumer.commit()
                except Exception:
                    log.exception("could not dead-letter order.created offset=%s", msg.offset)
                    await asyncio.sleep(2)
                    continue
                break
            except Exception:
                log.exception("failed to process order.created offset=%s", msg.offset)
                await asyncio.sleep(2)


async def publish_outbox(app: FastAPI):
    while True:
        try:
            async with app.state.db.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute("SELECT id,topic,message_key,payload FROM outbox_events WHERE published_at IS NULL ORDER BY created_at LIMIT 50")
                    rows = await cur.fetchall()
            for row in rows:
                payload = row["payload"]
                if isinstance(payload, str): payload = payload.encode()
                elif not isinstance(payload, bytes): payload = json.dumps(payload).encode()
                await app.state.producer.send_and_wait(row["topic"], payload, key=row["message_key"].encode())
                async with app.state.db.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("UPDATE outbox_events SET published_at=CURRENT_TIMESTAMP(6) WHERE id=%s AND published_at IS NULL", (row["id"],))
                    await conn.commit()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("warehouse outbox delivery failed")
        await asyncio.sleep(1)
