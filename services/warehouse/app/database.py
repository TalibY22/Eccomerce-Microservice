import asyncio
import hashlib
import os
from pathlib import Path

import aiomysql

MIGRATIONS = Path(__file__).resolve().parent.parent / "migrations"


async def apply_migrations(pool: aiomysql.Pool) -> None:
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT GET_LOCK('warehouse_migrations', 30)")
            lock = await cur.fetchone()
            if not lock or lock[0] != 1:
                raise RuntimeError("could not acquire warehouse migration lock")
            try:
                await cur.execute("""CREATE TABLE IF NOT EXISTS schema_migrations (
                    version VARCHAR(255) PRIMARY KEY,
                    checksum CHAR(64) NOT NULL,
                    statement_index INT UNSIGNED NOT NULL DEFAULT 0,
                    complete BOOLEAN NOT NULL DEFAULT FALSE,
                    started_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                    completed_at TIMESTAMP(6) NULL
                ) ENGINE=InnoDB""")
                await conn.commit()
                for path in sorted(MIGRATIONS.glob("*.sql")):
                    body = path.read_text()
                    checksum = hashlib.sha256(body.encode()).hexdigest()
                    await cur.execute("SELECT checksum, statement_index, complete FROM schema_migrations WHERE version=%s", (path.name,))
                    row = await cur.fetchone()
                    if row is None:
                        await cur.execute("INSERT INTO schema_migrations(version, checksum) VALUES(%s, %s)", (path.name, checksum))
                        await conn.commit()
                        index, complete = 0, 0
                    else:
                        if row[0] != checksum:
                            raise RuntimeError(f"applied migration {path.name} was modified")
                        index, complete = row[1], row[2]
                    if complete:
                        continue
                    for statement_index, statement in enumerate(body.split(";")):
                        statement = statement.strip()
                        if not statement or statement_index < index:
                            continue
                        await cur.execute(statement)
                        await conn.commit()
                        await cur.execute("UPDATE schema_migrations SET statement_index=%s WHERE version=%s", (statement_index + 1, path.name))
                        await conn.commit()
                    await cur.execute("UPDATE schema_migrations SET complete=TRUE, completed_at=CURRENT_TIMESTAMP(6) WHERE version=%s", (path.name,))
                    await conn.commit()
            finally:
                await cur.execute("SELECT RELEASE_LOCK('warehouse_migrations')")
                await cur.fetchone()


async def import_legacy_product_stock(pool: aiomysql.Pool) -> None:
    """Move old catalog stock into a default warehouse before product drops that column."""
    host = os.getenv("PRODUCT_DB_HOST")
    if not host:
        return
    product_db = await aiomysql.connect(
        host=host,
        port=int(os.getenv("PRODUCT_DB_PORT", "3306")),
        user=os.getenv("PRODUCT_DB_USER", "product_svc"),
        password=os.getenv("PRODUCT_DB_PASSWORD", "productpass"),
        db=os.getenv("PRODUCT_DB_NAME", "product_service"),
        autocommit=True,
        charset="utf8mb4",
    )
    try:
        async with product_db.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT COLUMN_NAME FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='products'")
            columns = {row["COLUMN_NAME"] for row in await cur.fetchall()}
            if "stock" not in columns:
                return
            sku_column = "sku" in columns
            select = "SELECT id, " + ("sku, " if sku_column else "") + "stock FROM products ORDER BY id"
            await cur.execute(select)
            products = await cur.fetchall()
    finally:
        product_db.close()

    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await conn.begin()
            warehouse_id = "00000000-0000-0000-0000-000000000001"
            await cur.execute("INSERT INTO warehouses(id,code,name) VALUES(%s,'DEFAULT','Default warehouse') ON DUPLICATE KEY UPDATE name=VALUES(name)", (warehouse_id,))
            for product in products:
                product_id = str(product["id"])
                quantity = int(product["stock"])
                if quantity < 0:
                    raise RuntimeError(f"legacy product {product_id} has negative stock")
                await cur.execute("SELECT product_id FROM legacy_inventory_imports WHERE product_id=%s", (product_id,))
                if await cur.fetchone():
                    continue
                sku = product.get("sku") or f"LEGACY-{product_id}"
                await cur.execute("INSERT INTO inventory_balances(warehouse_id,product_id,sku,quantity_on_hand) VALUES(%s,%s,%s,%s) ON DUPLICATE KEY UPDATE product_id=VALUES(product_id)", (warehouse_id,product_id,sku,quantity))
                if quantity:
                    await cur.execute("INSERT INTO stock_movements(warehouse_id,product_id,sku,movement_type,quantity_delta,reference_type,reference_id,reason) VALUES(%s,%s,%s,'legacy_import',%s,'migration','product-stock-v1','Imported from product catalog')", (warehouse_id,product_id,sku,quantity))
                await cur.execute("INSERT INTO legacy_inventory_imports(product_id,imported_quantity) VALUES(%s,%s)", (product_id,quantity))
            await conn.commit()
