from uuid import uuid4

from shared.runtime import enqueue_event


def _amount(event):
    for key in ("total_minor", "amount_minor", "amount"):
        value=event.get(key)
        if value is not None:
            try: return int(value)
            except (TypeError,ValueError): pass
    if event.get("total_amount") is not None:
        try: return int(round(float(event["total_amount"])*100))
        except (TypeError,ValueError): pass
    return 0

async def handle_event(cur,topic,event):
    kind=event.get("event_type",topic)
    order_id=event.get("order_id")
    if not order_id: return
    if kind in {"order.confirmed","order.created","order.fulfillment_ready","order.fulfilled","payment.captured","payment.succeeded"}:
        await cur.execute("SELECT order_id,user_id,currency,total_minor,paid,fulfilled,invoice_id FROM billing_orders WHERE order_id=%s FOR UPDATE",(str(order_id),)); old=await cur.fetchone()
        items=event.get("items") or []
        currency=str(event.get("currency") or (old[2] if old else "USD"))[:3].upper()
        amount=_amount(event) or (old[3] if old else 0)
        user_id=str(event.get("user_id") or (old[1] if old else "")) or None
        paid=bool(old[4]) if old else False; fulfilled=bool(old[5]) if old else False
        if kind in {"payment.captured","payment.succeeded"}: paid=True
        if kind in {"order.fulfillment_ready","order.fulfilled"}: fulfilled=True
        await cur.execute("INSERT INTO billing_orders(order_id,user_id,currency,total_minor,paid,fulfilled) VALUES(%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE user_id=COALESCE(VALUES(user_id),user_id),currency=VALUES(currency),total_minor=GREATEST(total_minor,VALUES(total_minor)),paid=VALUES(paid),fulfilled=VALUES(fulfilled)",(str(order_id),user_id,currency,amount,paid,fulfilled))
        if (not old or not old[6]) and items:
            inv_id=str(uuid4()); number="INV-"+inv_id[:8].upper()
            await cur.execute("INSERT INTO invoices(id,invoice_number,order_id,user_id,currency,subtotal_minor,total_minor) VALUES(%s,%s,%s,%s,%s,%s,%s)",(inv_id,number,str(order_id),user_id,currency,amount,amount))
            for item in items:
                try: qty=max(1,int(item.get("quantity",1))); raw_unit=item.get("unit_price_minor",item.get("price_minor")); unit=int(raw_unit) if raw_unit is not None else int(round(float(item.get("unit_price",0))*100))
                except (ValueError,TypeError): continue
                desc=str(item.get("name") or item.get("product_name") or item.get("product_id") or "Item")[:320]
                await cur.execute("INSERT INTO invoice_lines(invoice_id,description,quantity,unit_price_minor) VALUES(%s,%s,%s,%s)",(inv_id,desc,qty,unit))
            await cur.execute("UPDATE billing_orders SET invoice_id=%s WHERE order_id=%s",(inv_id,str(order_id)))
        await cur.execute("SELECT id,invoice_number,user_id,currency,total_minor,status FROM invoices WHERE order_id=%s FOR UPDATE",(str(order_id),)); invoice=await cur.fetchone()
        if invoice and (paid or fulfilled) and invoice[5]=="draft":
            await cur.execute("UPDATE invoices SET status='issued',issued_at=CURRENT_TIMESTAMP(6) WHERE id=%s",(invoice[0],))
            await enqueue_event(cur,"billing.events",str(order_id),{"event_type":"invoice.issued","invoice_id":invoice[0],"invoice_number":invoice[1],"order_id":str(order_id),"user_id":invoice[2],"currency":invoice[3],"total_minor":invoice[4]})
