import csv
import io
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from shared.runtime import enqueue_event,event_runtime
from .handlers import handle_event

@asynccontextmanager
async def lifespan(app):
    async with event_runtime(app,"billing",["order.created","order.confirmed","fulfillment.events","payment.events"],handle_event,"/app/migrations"):
        yield
app=FastAPI(title="Billing Service",lifespan=lifespan)
class CreditNote(BaseModel): amount_minor:int=Field(gt=0); reason:str=Field(min_length=3,max_length=500)

@app.get("/health")
def health(): return {"status":"ok"}

@app.get("/invoices/{order_id}")
async def get_invoice(order_id:str,request:Request):
    async with request.app.state.db.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT id,invoice_number,order_id,user_id,currency,subtotal_minor,tax_minor,total_minor,status,issued_at FROM invoices WHERE order_id=%s",(order_id,)); row=await cur.fetchone()
            if not row: raise HTTPException(404,"invoice not found")
            await cur.execute("SELECT description,quantity,unit_price_minor,tax_rate_bps,tax_minor FROM invoice_lines WHERE invoice_id=%s",(row[0],)); lines=await cur.fetchall()
    return {"id":row[0],"invoice_number":row[1],"order_id":row[2],"user_id":row[3],"currency":row[4],"subtotal_minor":row[5],"tax_minor":row[6],"total_minor":row[7],"status":row[8],"issued_at":row[9],"lines":[dict(zip(["description","quantity","unit_price_minor","tax_rate_bps","tax_minor"],x)) for x in lines]}

@app.post("/invoices/{invoice_id}/credit-notes")
async def create_credit(invoice_id:str,value:CreditNote,request:Request):
    async with request.app.state.db.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT invoice_number,order_id,user_id,currency,total_minor,status FROM invoices WHERE id=%s FOR UPDATE",(invoice_id,)); inv=await cur.fetchone()
            if not inv: raise HTTPException(404,"invoice not found")
            if inv[5]!="issued": raise HTTPException(409,"invoice must be issued")
            await cur.execute("SELECT COALESCE(SUM(amount_minor),0) FROM credit_notes WHERE invoice_id=%s",(invoice_id,)); credited=(await cur.fetchone())[0]
            if credited+value.amount_minor>inv[4]: raise HTTPException(422,"credit exceeds invoice total")
            ident=str(uuid4()); number="CN-"+ident[:8].upper()
            await cur.execute("INSERT INTO credit_notes(id,credit_note_number,invoice_id,amount_minor,reason) VALUES(%s,%s,%s,%s,%s)",(ident,number,invoice_id,value.amount_minor,value.reason))
            await enqueue_event(cur,"billing.events",inv[1],{"event_type":"credit_note.issued","credit_note_id":ident,"credit_note_number":number,"invoice_id":invoice_id,"order_id":inv[1],"user_id":inv[2],"currency":inv[3],"amount_minor":value.amount_minor,"reason":value.reason})
        await conn.commit()
    return {"id":ident,"credit_note_number":number,"status":"issued"}

@app.post("/exports")
async def export(request:Request):
    async with request.app.state.db.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT invoice_number,order_id,user_id,currency,subtotal_minor,tax_minor,total_minor,status,issued_at FROM invoices ORDER BY created_at"); rows=await cur.fetchall()
            stream=io.StringIO(); writer=csv.writer(stream); writer.writerow(["invoice_number","order_id","user_id","currency","subtotal_minor","tax_minor","total_minor","status","issued_at"]); writer.writerows(rows)
            content=stream.getvalue(); ident=str(uuid4())
            await cur.execute("INSERT INTO accounting_exports(id,format,status,row_count,content) VALUES(%s,'csv','completed',%s,%s)",(ident,len(rows),content))
            await enqueue_event(cur,"billing.events",ident,{"event_type":"accounting_export.completed","export_id":ident,"format":"csv","row_count":len(rows)})
        await conn.commit()
    return {"id":ident,"format":"csv","status":"completed","row_count":len(rows),"content":content}
