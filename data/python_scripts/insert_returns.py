# ------------------------------------------------------------------------------------------------------------------------------------
# DEPENDENCIES
# ------------------------------------------------------------------------------------------------------------------------------------

import os
import time
import random
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Mapping, Sequence, TypeVar, Union

import mysql.connector
from datetime import date, datetime, time as dtime, timedelta
from dotenv import load_dotenv

load_dotenv()

# ------------------------------------------------------------------------------------------------------------------------------------
# UTILS
# ------------------------------------------------------------------------------------------------------------------------------------

K = TypeVar("K")

def clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))

def normalize_distribution(dist: Mapping[K, Union[int, float]]) -> dict[K, float]:
    """Normaliza {chave: peso} para somar 1.0. Recusa negativos e soma <= 0."""
    if not dist:
        raise ValueError("Distribution is empty.")
    if any(v < 0 for v in dist.values()):
        raise ValueError("Distribution weights cannot be negative.")
    total = float(sum(dist.values()))
    if total <= 0:
        raise ValueError("Distribution must have positive weights.")
    return {k: float(v) / total for k, v in dist.items()}

def weighted_choice_key(rng: random.Random, weights: Mapping[K, float]) -> K:
    """Escolhe uma chave segundo pesos (não precisam já de estar normalizados)."""
    if not weights:
        raise ValueError("Empty weights.")
    keys = list(weights.keys())
    vals = list(weights.values())
    if any(v < 0 for v in vals) or sum(vals) <= 0:
        raise ValueError("Invalid weights (negatives or non-positive sum).")
    return rng.choices(keys, weights=vals, k=1)[0]

# ------------------------------------------------------------------------------------------------------------------------------------
# CONFIGS
# ------------------------------------------------------------------------------------------------------------------------------------

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASS", ""),
    "database": os.getenv("DB_NAME", "ecommerce_db_test"),
    "charset": "utf8mb4",
}

RANDOM_SEED = 42

# % de encomendas (pagas + delivered/cancelled) que terão pelo menos uma devolução
ORDER_LEVEL_RETURN_RATE = 0.40

# multiplicadores por país do cliente
COUNTRY_RETURN_MULTIPLIER: dict[str, float] = {
    "PT": 1.00, "ES": 0.90, "FR": 1.10, "DE": 1.20, "IT": 0.80,
    "NL": 0.95, "BE": 1.00, "GR": 0.85, "HR": 1.10, "IE": 1.25,
}

# probabilidade média por categoria de um order_item ser devolvido
CATEGORY_ITEM_RETURN_RATE: dict[str, float] = {
    "Electronics": 0.20, "Fashion": 0.35, "Home & Kitchen": 0.10, "Beauty & Personal Care": 0.07,
    "Sports & Fitness": 0.12, "Books": 0.04, "Toys": 0.11, "Gardening": 0.08, "Automotive": 0.09, "Pet Supplies": 0.07,
}

# distribuição de razões por categoria
CATEGORY_REASON_DISTS: dict[str, dict[str, float]] = {
    "Electronics": {"damaged": 0.28, "not_as_described": 0.34, "late": 0.08, "change_of_mind": 0.22, "other": 0.08},
    "Fashion": {"damaged": 0.07, "not_as_described": 0.26, "late": 0.07, "change_of_mind": 0.53, "other": 0.07},
    "Home & Kitchen": {"damaged": 0.22, "not_as_described": 0.27, "late": 0.10, "change_of_mind": 0.31, "other": 0.10},
    "Beauty & Personal Care": {"damaged": 0.24, "not_as_described": 0.20, "late": 0.09, "change_of_mind": 0.37, "other": 0.10},
    "Sports & Fitness": {"damaged": 0.20, "not_as_described": 0.25, "late": 0.10, "change_of_mind": 0.35, "other": 0.10},
    "Books": {"damaged": 0.35, "not_as_described": 0.15, "late": 0.10, "change_of_mind": 0.30, "other": 0.10},
    "Toys": {"damaged": 0.22, "not_as_described": 0.23, "late": 0.10, "change_of_mind": 0.35, "other": 0.10},
    "Gardening": {"damaged": 0.23, "not_as_described": 0.22, "late": 0.12, "change_of_mind": 0.33, "other": 0.10},
    "Automotive": {"damaged": 0.15, "not_as_described": 0.45, "late": 0.07, "change_of_mind": 0.25, "other": 0.08},
    "Pet Supplies": {"damaged": 0.20, "not_as_described": 0.22, "late": 0.08, "change_of_mind": 0.40, "other": 0.10},
}

MAX_ITEMS_PER_ORDER = 5
RETURN_MIN_DAYS = 3
RETURN_MAX_DAYS = 30

ALLOWED_ORDER_STATUS_CODES = ("delivered", "cancelled")
PAID_STATUS_CODES = ("paid",)

BATCH_SIZE = 20_000
CLEAR_EXISTING_FOR_ITEMS = True  # idempotência por item

# ------------------------------------------------------------------------------------------------------------------------------------
# DATABASE
# ------------------------------------------------------------------------------------------------------------------------------------

def get_connection() -> "mysql.connector.connection.MySQLConnection":
    return mysql.connector.connect(use_pure=True, **DB_CONFIG)

# ------------------------------------------------------------------------------------------------------------------------------------
# DATA MODELS
# ------------------------------------------------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class CandidateItem:
    order_item_id: int
    order_id: int
    product_id: int
    quantity: int
    unit_price: Decimal
    category_name: str
    customer_iso: str
    last_paid_at: datetime

@dataclass(frozen=True)
class LookupMaps:
    reason_code_to_id: dict[str, int]
    category_name_to_id: dict[str, int]
    country_iso_to_id: dict[str, int]
    
# ------------------------------------------------------------------------------------------------------------------------------------
# FETCHERS
# ------------------------------------------------------------------------------------------------------------------------------------

def fetch_lookup_maps(cur: "mysql.connector.cursor.MySQLCursor") -> LookupMaps:
    cur.execute("SELECT return_reason_id, code FROM return_reasons ORDER BY return_reason_id ASC")
    reasons = cur.fetchall()
    if not reasons:
        raise RuntimeError("Table 'return_reasons' is empty.")
    reason_map = {str(code): int(rid) for rid, code in reasons}

    cur.execute("SELECT category_id, name FROM product_categories ORDER BY category_id ASC")
    cats = cur.fetchall()
    if not cats:
        raise RuntimeError("Table 'product_categories' is empty.")
    category_map = {str(name): int(cid) for cid, name in cats}

    cur.execute("SELECT country_id, iso_code FROM countries ORDER BY country_id ASC")
    countries = cur.fetchall()
    if not countries:
        raise RuntimeError("Table 'countries' is empty.")
    country_map = {str(iso): int(cid) for cid, iso in countries}

    return LookupMaps(reason_map, category_map, country_map)

def fetch_candidate_items(conn) -> list[CandidateItem]:
    sql = f"""
    WITH paid_orders AS (
        SELECT p.order_id, MAX(p.payment_date) AS last_paid_at
        FROM payments p
        JOIN payment_status ps ON ps.payment_status_id = p.payment_status_id
        WHERE ps.code IN ({",".join(["%s"]*len(PAID_STATUS_CODES))})
        GROUP BY p.order_id
    ),
    eligible_orders AS (
        SELECT o.order_id, po.last_paid_at, os.code AS order_status_code, ctry.iso_code AS customer_iso
        FROM orders o
        JOIN order_status os ON os.order_status_id = o.order_status_id
        JOIN customers cu ON cu.customer_id = o.customer_id
        JOIN countries ctry ON ctry.country_id = cu.country_id
        JOIN paid_orders po ON po.order_id = o.order_id
        WHERE os.code IN ({",".join(["%s"]*len(ALLOWED_ORDER_STATUS_CODES))})
    )
    SELECT
        oi.order_item_id,
        oi.order_id,
        oi.product_id,
        oi.quantity,
        oi.unit_price,
        pc.name AS category_name,
        eo.customer_iso,
        eo.last_paid_at
    FROM order_items oi
    JOIN products pr ON pr.product_id = oi.product_id
    JOIN product_categories pc ON pc.category_id = pr.category_id
    JOIN eligible_orders eo ON eo.order_id = oi.order_id
    ORDER BY oi.order_item_id ASC
    """
    params = list(PAID_STATUS_CODES) + list(ALLOWED_ORDER_STATUS_CODES)

    with conn.cursor(dictionary=True) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    out: list[CandidateItem] = [
        CandidateItem(
            order_item_id=int(r["order_item_id"]),
            order_id=int(r["order_id"]),
            product_id=int(r["product_id"]),
            quantity=int(r["quantity"]),
            unit_price=Decimal(str(r["unit_price"])),
            category_name=str(r["category_name"]),
            customer_iso=str(r["customer_iso"]),
            last_paid_at=r["last_paid_at"],
        )
        for r in rows
    ]
    return out

# ------------------------------------------------------------------------------------------------------------------------------------
# IDEMPOTÊNCIA & PERSISTÊNCIA
# ------------------------------------------------------------------------------------------------------------------------------------

def delete_returns_for_items(cur: "mysql.connector.cursor.MySQLCursor", order_item_ids: Sequence[int]) -> None:
    if not order_item_ids:
        return
    placeholders = ",".join(["%s"] * len(order_item_ids))
    cur.execute(f"DELETE FROM product_returns WHERE order_item_id IN ({placeholders})", list(order_item_ids))

SQL_INSERT = """
INSERT INTO product_returns (order_item_id, return_date, refund_amount, return_reason_id)
VALUES (%s, %s, %s, %s)
"""

def insert_returns_in_batches(
    conn: "mysql.connector.connection.MySQLConnection",
    rows: Sequence[tuple[int, datetime, Decimal, int]],
    batch_size: int,
) -> tuple[int, int]:
    total = 0
    batches = 0
    if not rows:
        return total, batches

    with conn.cursor() as cur:
        buf: list[tuple[int, datetime, Decimal, int]] = []
        for rec in rows:
            buf.append(rec)
            if batch_size and len(buf) >= batch_size:
                cur.executemany(SQL_INSERT, buf)
                conn.commit()
                total += len(buf)
                batches += 1
                buf.clear()

        if buf:
            cur.executemany(SQL_INSERT, buf)
            conn.commit()
            total += len(buf)
            batches += 1

    return total, batches

# ------------------------------------------------------------------------------------------------------------------------------------
# GERAÇÃO
# ------------------------------------------------------------------------------------------------------------------------------------

def pick_orders_to_return(
    rng: random.Random,
    items_by_order: Mapping[int, list[CandidateItem]],
) -> set[int]:
    selected: set[int] = set()
    for order_id, items in items_by_order.items():
        country_iso = items[0].customer_iso  # país é estável por encomenda
        mult = COUNTRY_RETURN_MULTIPLIER.get(country_iso, 1.0)
        p = clamp01(ORDER_LEVEL_RETURN_RATE * mult)
        if rng.random() < p:
            selected.add(order_id)
    return selected

def pick_items_for_order(
    rng: random.Random,
    items: Sequence[CandidateItem],
    category_rates: Mapping[str, float],
    max_items: int,
) -> list[CandidateItem]:
    picked: list[CandidateItem] = []
    for it in items:
        rate = clamp01(category_rates.get(it.category_name, 0.0))
        if rng.random() < rate:
            picked.append(it)

    if len(picked) > max_items:
        picked = rng.sample(picked, k=max_items)

    if not picked and items:
        # garante pelo menos 1 item, ponderando pela taxa de categoria
        weights = [category_rates.get(it.category_name, 0.01) + 1e-6 for it in items]
        weights_norm = normalize_distribution({i: w for i, w in enumerate(weights)})
        chosen_index = weighted_choice_key(rng, weights_norm)
        picked = [items[int(chosen_index)]]

    return picked

def choose_reason_for_item(
    rng: random.Random,
    category_name: str,
    reason_weights_by_category: Mapping[str, Mapping[str, float]],
    fallback_reason_map: Mapping[str, int],
) -> int:
    dist = reason_weights_by_category.get(category_name, {"other": 1.0})
    # normaliza para estabilidade
    dist_norm = normalize_distribution(dist)
    chosen_code = weighted_choice_key(rng, dist_norm)
    return (
        fallback_reason_map.get(chosen_code)
        or fallback_reason_map.get("other")
        or next(iter(fallback_reason_map.values()))
    )

def build_return_rows(
    rng: random.Random,
    items: Sequence[CandidateItem],
    reason_code_to_id: Mapping[str, int],
) -> list[tuple[int, datetime, Decimal, int]]:
    rows: list[tuple[int, datetime, Decimal, int]] = []
    for it in items:
        paid_date: date = it.last_paid_at.date()
        delta_days = rng.randint(RETURN_MIN_DAYS, RETURN_MAX_DAYS)
        return_dt = datetime.combine(paid_date + timedelta(days=delta_days), dtime(12, 0, 0))

        unit_price = it.unit_price
        qty = int(it.quantity)
        refund_amount = (unit_price * qty).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        reason_id = choose_reason_for_item(rng, it.category_name, CATEGORY_REASON_DISTS, reason_code_to_id)
        rows.append((it.order_item_id, return_dt, refund_amount, reason_id))
    return rows

# ------------------------------------------------------------------------------------------------------------------------------------
# ORQUESTRAÇÃO
# ------------------------------------------------------------------------------------------------------------------------------------

def run(seed: int = RANDOM_SEED) -> None:
    rng = random.Random(seed)
    print(f"🔌 A ligar à BD '{DB_CONFIG['database']}' como '{DB_CONFIG['user']}' em '{DB_CONFIG['host']}'...")

    conn = get_connection()
    conn.autocommit = False

    try:
        # 1) Cursor “normal” para lookups + fixar UTC
        with conn.cursor() as cur:
            cur.execute("SET time_zone = '+00:00';")
            lookups = fetch_lookup_maps(cur)

        # 2) Cursor “dictionary=True” só dentro da função que precisa
        candidates = fetch_candidate_items(conn)

        if not candidates:
            print("⚠️ Não existem order_items elegíveis para devolução.")
            return

        # 3) Agrupar por encomenda e escolher quais devolvem
        items_by_order: dict[int, list[CandidateItem]] = {}
        for it in candidates:
            items_by_order.setdefault(it.order_id, []).append(it)
            
        # ordem cronológica por data do último pagamento
        order_ids_sorted = sorted(
            items_by_order.keys(),
            key=lambda oid: items_by_order[oid][0].last_paid_at
        )

        # escolher que encomendas devolvem (probabilístico)
        selected_orders = pick_orders_to_return(rng, items_by_order)
        
        # manter apenas as selecionadas, na MESMA ordem cronológica
        selected_order_ids_sorted = [oid for oid in order_ids_sorted if oid in selected_orders]

        # 4) Escolher itens por encomenda (seguindo a ordem cronológica)
        to_return_items: list[CandidateItem] = []
        for oid in selected_order_ids_sorted:
            to_return_items.extend(
                pick_items_for_order(
                    rng,
                    items_by_order[oid],
                    CATEGORY_ITEM_RETURN_RATE,
                    MAX_ITEMS_PER_ORDER
                )
            )

        if not to_return_items:
            print("⚠️ Nenhum order_item selecionado para devolução. Ajusta CATEGORY_ITEM_RETURN_RATE/ORDER_LEVEL_RETURN_RATE.")
            return

        # 5) Idempotência por item
        if CLEAR_EXISTING_FOR_ITEMS:
            with conn.cursor() as cur:
                delete_returns_for_items(cur, [it.order_item_id for it in to_return_items])
                conn.commit()
                

        # 6) Construir registos e inserir em batches (ordenar por return_date)
        records = build_return_rows(rng, to_return_items, lookups.reason_code_to_id)
        records.sort(key=lambda r: r[1])  # r[1] = return_date

        start = time.perf_counter()
        total, batches = insert_returns_in_batches(conn, records, BATCH_SIZE)
        elapsed = time.perf_counter() - start

        print(f"✅ Inseridos {total} registos em product_returns para {len(selected_order_ids_sorted)} encomendas (em {batches} batch(es)).")
        if elapsed > 0:
            print(f"⏱️ Tempo de inserção: {elapsed:.2f}s (~{total/elapsed:.1f} rows/s)")

    except mysql.connector.Error as e:
        conn.rollback()
        print(f"[MySQL] {e.__class__.__name__}: {e}")
        return
    except RuntimeError as e:
        conn.rollback()
        print(f"[Runtime] {e.__class__.__name__}: {e}")
        return
    finally:
        conn.close()
        
# ------------------------------------------------------------------------------------------------------------------------------------
# ENTRYPOINT
# ------------------------------------------------------------------------------------------------------------------------------------

def main() -> None:
    run(RANDOM_SEED)

if __name__ == "__main__":
    main()