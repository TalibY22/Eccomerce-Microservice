import json
from uuid import uuid4

from .providers import deliver
from shared.runtime import enqueue_event


async def handle_event(cur, topic, event):
    event_type = event.get("event_type", topic)
    user_id = event.get("user_id") or event.get("customer_id")
    if not user_id:
        return
    values = {k: str(v) for k, v in event.items() if isinstance(v, (str, int, float, bool))}
    fallback = f"{event_type.replace('.', ' ').title()} for order {event.get('order_id', '')}".strip()
    for channel in ("email", "sms", "push"):
        await cur.execute("SELECT destination FROM notification_contacts WHERE user_id=%s AND channel=%s", (str(user_id), channel))
        contact = await cur.fetchone()
        await cur.execute("SELECT enabled FROM notification_preferences WHERE user_id=%s AND channel=%s", (str(user_id), channel))
        pref = await cur.fetchone()
        if not contact or (pref and not pref[0]):
            continue
        await cur.execute("SELECT subject_template,body_template FROM notification_templates WHERE event_type=%s AND channel=%s", (event_type, channel))
        template = await cur.fetchone()
        subject = template[0].format_map(_Safe(values)) if template and template[0] else fallback
        body = template[1].format_map(_Safe(values)) if template else json.dumps(event, ensure_ascii=False)
        await cur.execute("""INSERT IGNORE INTO notification_attempts(id,event_id,user_id,event_type,channel,destination,subject,body)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s)""", (str(uuid4()), event["event_id"], str(user_id), event_type, channel, contact[0], subject, body))


class _Safe(dict):
    def __missing__(self, key): return ""


async def send_pending(pool, producer):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT id,event_id,user_id,event_type,channel,destination,subject,body FROM notification_attempts WHERE status IN ('pending','failed') AND attempts < 8 ORDER BY created_at LIMIT 50")
            items = await cur.fetchall()
    for item in items:
        ident,event_id,user_id,event_type,channel,destination,subject,body = item
        try:
            message_id = await deliver(channel,destination,subject,body)
            status, error = "delivered", None
        except Exception as exc:
            message_id, status, error = None, "failed", str(exc)[:2000]
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await conn.begin()
                await cur.execute("UPDATE notification_attempts SET status=%s,provider_message_id=%s,error=%s,attempts=attempts+1 WHERE id=%s AND status IN ('pending','failed')", (status,message_id,error,ident))
                if cur.rowcount:
                    await enqueue_event(cur,"notification.events",str(user_id),{
                        "event_type": "notification.delivery." + status,
                        "notification_id": ident, "source_event_id": event_id, "user_id": user_id,
                        "channel": channel, "provider_message_id": message_id, "error": error})
                await conn.commit()
