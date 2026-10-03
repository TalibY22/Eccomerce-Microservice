import json
from uuid import uuid4

from shared.runtime import enqueue_event


async def handle_event(cur, topic, event):
    event_type = event.get("event_type", topic)
    if event_type not in {"order.confirmed", "order.fulfillment_ready", "fulfillment.ready"}:
        return
    order_id = str(event.get("order_id", ""))
    if not order_id: return
    shipment_id = str(uuid4())
    await cur.execute("INSERT IGNORE INTO shipments(id,order_id,user_id,status) VALUES(%s,%s,%s,'awaiting_address')", (shipment_id,order_id,event.get("user_id")))
    await cur.execute("SELECT id,status FROM shipments WHERE order_id=%s", (order_id,))
    row=await cur.fetchone(); shipment_id,status=row
    await cur.execute("INSERT IGNORE INTO delivery_timeline(shipment_id,event_key,status,description) VALUES(%s,%s,%s,%s)", (shipment_id,"created:"+event["event_id"],status,"Shipment created from fulfillment event"))
    await enqueue_event(cur,"delivery.events",order_id,{"event_type":"delivery.created","shipment_id":shipment_id,"order_id":order_id,"user_id":event.get("user_id"),"status":status})
