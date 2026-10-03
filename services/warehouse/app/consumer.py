import json
from uuid import uuid4

import aiomysql
from pydantic import ValidationError

from app.events.events import OrderCreatedEvent


async def process_order_created(pool: aiomysql.Pool, event: dict) -> None:
    """Confirm stock atomically, then queue an at-least-once outbox event."""
    if not isinstance(event, dict):
        raise ValueError("order.created must be a JSON object")
    try:
        event = OrderCreatedEvent.model_validate(event).model_dump()
    except ValidationError as exc:
        raise ValueError(f"invalid order.created event: {exc}") from exc
    order_id = event["order_id"]
    items = event["items"]
    requested: dict[str, int] = {}
    item_metadata = {}
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("order.created contains an invalid item")
        product_id, quantity = item.get("product_id"), item.get("quantity")
        if not product_id or not isinstance(quantity, int) or quantity < 1:
            raise ValueError("order.created contains an invalid item")
        requested[product_id] = requested.get(product_id, 0) + quantity
        item_metadata[product_id] = item

    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await conn.begin()
            await cur.execute("SELECT outcome FROM processed_orders WHERE order_id=%s FOR UPDATE", (order_id,))
            if await cur.fetchone():
                await conn.commit()
                return

            await cur.execute("SELECT state FROM order_reservations WHERE order_id=%s FOR UPDATE", (order_id,))
            reservation = await cur.fetchone()
            outcome = "confirmed"
            allocations = []
            if reservation and reservation["state"] in {"reserved", "committed"}:
                await cur.execute("SELECT warehouse_id,product_id,sku,quantity FROM order_reservation_items WHERE order_id=%s ORDER BY warehouse_id,product_id", (order_id,))
                allocations = await cur.fetchall()
                if reservation["state"] == "reserved":
                    for row in allocations:
                        await cur.execute("UPDATE inventory_balances SET quantity_on_hand=quantity_on_hand-%s,quantity_reserved=quantity_reserved-%s WHERE warehouse_id=%s AND product_id=%s", (row["quantity"],row["quantity"],row["warehouse_id"],row["product_id"]))
                        await movement(cur,row["warehouse_id"],row["product_id"],row["sku"],"reservation_committed",-row["quantity"],-row["quantity"],order_id)
                    await cur.execute("UPDATE order_reservations SET state='committed' WHERE order_id=%s", (order_id,))
            elif reservation:
                outcome = "rejected"
            else:
                for product_id, quantity in sorted(requested.items()):
                    await cur.execute("""SELECT warehouse_id,sku,quantity_on_hand,quantity_reserved,reorder_level
                        FROM inventory_balances WHERE product_id=%s ORDER BY warehouse_id FOR UPDATE""", (product_id,))
                    rows = await cur.fetchall()
                    if sum(row["quantity_on_hand"]-row["quantity_reserved"] for row in rows) < quantity:
                        outcome = "rejected"
                        break
                    remaining = quantity
                    for row in rows:
                        take = min(remaining,row["quantity_on_hand"]-row["quantity_reserved"])
                        if take:
                            allocations.append({"warehouse_id":row["warehouse_id"],"product_id":product_id,"sku":row["sku"],"quantity":take,"reorder_level":row["reorder_level"],"on_hand":row["quantity_on_hand"],"reserved":row["quantity_reserved"]})
                            remaining -= take
                        if remaining == 0: break
                await cur.execute("INSERT INTO order_reservations(order_id,state) VALUES(%s,%s)", (order_id,"committed" if outcome == "confirmed" else "rejected"))
                if outcome == "confirmed":
                    for row in allocations:
                        await cur.execute("UPDATE inventory_balances SET quantity_on_hand=quantity_on_hand-%s WHERE warehouse_id=%s AND product_id=%s", (row["quantity"],row["warehouse_id"],row["product_id"]))
                        await cur.execute("INSERT INTO order_reservation_items(order_id,warehouse_id,product_id,sku,quantity) VALUES(%s,%s,%s,%s,%s)", (order_id,row["warehouse_id"],row["product_id"],row["sku"],row["quantity"]))
                        await movement(cur,row["warehouse_id"],row["product_id"],row["sku"],"order_fulfillment",-row["quantity"],0,order_id)
                        previous_available = row["on_hand"] - row["reserved"]
                        available = previous_available - row["quantity"]
                        if previous_available > row["reorder_level"] >= available:
                            await queue_event(cur,"inventory.events",row["product_id"],{"event_id":str(uuid4()),"event_type":"inventory.low_stock","warehouse_id":row["warehouse_id"],"product_id":row["product_id"],"sku":row["sku"],"available":available,"reorder_level":row["reorder_level"]})

            if outcome == "confirmed" and allocations:
                confirmed_items = []
                for row in allocations:
                    source = item_metadata.get(row["product_id"], {})
                    confirmed_items.append({"product_id":row["product_id"],"sku":row["sku"],"product_name":source.get("product_name", ""),"quantity":row["quantity"],"unit_price":source.get("unit_price", 0)})
            else:
                confirmed_items = items
            await cur.execute("INSERT INTO processed_orders(order_id,outcome) VALUES(%s,%s)", (order_id,outcome))
            result = {"event_id":str(uuid4()),"event_type":f"order.{outcome}","schema_version":1,"order_id":order_id,"user_id":event.get("user_id",""),"items":confirmed_items,"total_amount":event.get("total_amount",0),"currency":event.get("currency","USD"),"status":outcome,"created_at":event.get("created_at","")}
            await queue_event(cur,"order.confirmed" if outcome == "confirmed" else "order.rejected",order_id,result)
        await conn.commit()


async def movement(cur, warehouse_id, product_id, sku, kind, quantity_delta, reserved_delta, order_id):
    await cur.execute("INSERT INTO stock_movements(warehouse_id,product_id,sku,movement_type,quantity_delta,reserved_delta,reference_type,reference_id) VALUES(%s,%s,%s,%s,%s,%s,'order',%s)", (warehouse_id,product_id,sku,kind,quantity_delta,reserved_delta,order_id))


async def queue_event(cur, topic: str, key: str, event: dict) -> None:
    await cur.execute("INSERT INTO outbox_events(id,topic,message_key,payload) VALUES(%s,%s,%s,%s)", (str(uuid4()),topic,key,json.dumps(event,separators=(",",":"))))
