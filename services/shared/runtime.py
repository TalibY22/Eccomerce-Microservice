import asyncio
import hashlib
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import aiomysql
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

log = logging.getLogger("ecommerce.runtime")


async def create_pool(prefix: str = "DB"):
    return await aiomysql.create_pool(
        host=_env(prefix, "HOST", "localhost"),
        port=int(_env(prefix, "PORT", "3306")),
        user=_env(prefix, "USER", "app"),
        password=_env(prefix, "PASSWORD", "apppass"),
        db=_env(prefix, "NAME", "app_service"),
        minsize=1,
        maxsize=10,
        autocommit=False,
        charset="utf8mb4",
    )


def _env(prefix: str, suffix: str, fallback: str) -> str:
    import os
    return os.getenv(f"{prefix}_{suffix}", fallback)


async def apply_migrations(pool, directory: str | Path, lock_name: str) -> None:
    paths = sorted(Path(directory).glob("*.sql"))
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT GET_LOCK(%s, 30)", (lock_name,))
            if (await cur.fetchone())[0] != 1:
                raise RuntimeError(f"could not acquire migration lock {lock_name}")
            try:
                await cur.execute("""CREATE TABLE IF NOT EXISTS schema_migrations (
                    version VARCHAR(255) PRIMARY KEY, checksum CHAR(64) NOT NULL,
                    statement_index INT UNSIGNED NOT NULL DEFAULT 0, complete BOOLEAN NOT NULL DEFAULT FALSE,
                    started_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), completed_at TIMESTAMP(6) NULL
                ) ENGINE=InnoDB""")
                await conn.commit()
                for path in paths:
                    body = path.read_text()
                    checksum = hashlib.sha256(body.encode()).hexdigest()
                    await cur.execute("SELECT checksum,statement_index,complete FROM schema_migrations WHERE version=%s", (path.name,))
                    row = await cur.fetchone()
                    if row is None:
                        await cur.execute("INSERT INTO schema_migrations(version,checksum) VALUES(%s,%s)", (path.name,checksum))
                        await conn.commit()
                        index, complete = 0, False
                    else:
                        if row[0] != checksum:
                            raise RuntimeError(f"applied migration {path.name} was changed")
                        index, complete = row[1], row[2]
                    if complete:
                        continue
                    statements = body.split(";")
                    for i, statement in enumerate(statements):
                        statement = statement.strip()
                        if not statement or i < index:
                            continue
                        await cur.execute(statement)
                        await conn.commit()
                        await cur.execute("UPDATE schema_migrations SET statement_index=%s WHERE version=%s", (i + 1,path.name))
                        await conn.commit()
                    await cur.execute("UPDATE schema_migrations SET complete=TRUE,completed_at=CURRENT_TIMESTAMP(6) WHERE version=%s", (path.name,))
                    await conn.commit()
            finally:
                await cur.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
                await cur.fetchone()


async def enqueue_event(cur, topic: str, key: str, event: dict) -> str:
    event_id = event.setdefault("event_id", str(uuid4()))
    event.setdefault("schema_version", 1)
    await cur.execute(
        "INSERT INTO event_outbox(id,topic,message_key,payload) VALUES(%s,%s,%s,%s)",
        (event_id, topic, key, json.dumps(event, separators=(",", ":"))),
    )
    return event_id


async def _process(pool, topic: str, event: dict, handler) -> None:
    event_id = event.get("event_id")
    if not event_id:
        raise ValueError("event is missing event_id")
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await conn.begin()
            await cur.execute("SELECT event_id FROM event_inbox WHERE event_id=%s FOR UPDATE", (event_id,))
            if await cur.fetchone():
                await conn.commit()
                return
            await handler(cur, topic, event)
            await cur.execute("INSERT INTO event_inbox(event_id,topic) VALUES(%s,%s)", (event_id,topic))
        await conn.commit()


async def _consume(pool, consumer, producer, handler) -> None:
    async for message in consumer:
        while True:
            try:
                event = json.loads(message.value)
                if not isinstance(event, dict):
                    raise ValueError("event payload must be a JSON object")
                if not event.get("event_id"):
                    seed = f"{message.topic}:{message.partition}:{message.offset}".encode()
                    event["event_id"] = hashlib.sha256(seed).hexdigest()
                await _process(pool, message.topic, event, handler)
                await consumer.commit()
                break
            except (json.JSONDecodeError, ValueError) as exc:
                raw = message.value.decode("utf-8", errors="replace") if isinstance(message.value, bytes) else repr(message.value)
                try:
                    await producer.send_and_wait(f"{message.topic}.dlq", json.dumps({"error":str(exc),"raw":raw}).encode(), key=message.key)
                    await consumer.commit()
                except Exception:
                    log.exception("could not dead-letter %s offset=%s", message.topic, message.offset)
                    await asyncio.sleep(2)
                    continue
                break
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("consumer processing failed topic=%s offset=%s", message.topic, message.offset)
                await asyncio.sleep(2)


async def _publish(pool, producer) -> None:
    while True:
        try:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute("SELECT id,topic,message_key,payload FROM event_outbox WHERE published_at IS NULL ORDER BY created_at LIMIT 50")
                    rows = await cur.fetchall()
            for row in rows:
                payload = row["payload"]
                if isinstance(payload, str): payload = payload.encode()
                elif not isinstance(payload, bytes): payload = json.dumps(payload).encode()
                await producer.send_and_wait(row["topic"], payload, key=row["message_key"].encode())
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("UPDATE event_outbox SET published_at=CURRENT_TIMESTAMP(6) WHERE id=%s AND published_at IS NULL", (row["id"],))
                    await conn.commit()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("outbox publishing failed")
        await asyncio.sleep(1)


@asynccontextmanager
async def event_runtime(app, service: str, topics: list[str], handler, migrations: str | Path):
    import os
    pool = await create_pool(service.upper())
    await apply_migrations(pool, migrations, f"{service}_migrations")
    broker = os.getenv("KAFKA_BROKER", "localhost:9094")
    producer = AIOKafkaProducer(bootstrap_servers=broker, enable_idempotence=True)
    consumer = AIOKafkaConsumer(*topics, bootstrap_servers=broker, group_id=f"{service}-v1", enable_auto_commit=False, auto_offset_reset="earliest")
    await producer.start()
    try:
        await consumer.start()
    except Exception:
        await producer.stop()
        pool.close()
        await pool.wait_closed()
        raise
    app.state.db = pool
    app.state.producer = producer
    tasks = [asyncio.create_task(_consume(pool, consumer, producer, handler)), asyncio.create_task(_publish(pool, producer))]
    try:
        yield
    finally:
        for task in tasks: task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await consumer.stop()
        await producer.stop()
        pool.close()
        await pool.wait_closed()
