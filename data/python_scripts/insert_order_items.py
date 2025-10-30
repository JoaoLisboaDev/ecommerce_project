# ------------------------------------------------------------------------------------------------------------------------------------
# DEPENDÊNCIAS
# ------------------------------------------------------------------------------------------------------------------------------------

import os
import random
import time
from decimal import Decimal
from dataclasses import dataclass
from typing import  Mapping, Sequence, TypeVar, Union

import mysql.connector
from dotenv import load_dotenv

load_dotenv()

# ------------------------------------------------------------------------------------------------------------------------------------
# UTILS
# ------------------------------------------------------------------------------------------------------------------------------------

K = TypeVar("K")

def normalize_distribution(dist: Mapping[K, Union[int, float]]) -> dict[K, float]:
    """
    Normaliza {chave: peso} para somar 1.0. Recusa negativos e soma <= 0.
    """
    if not dist:
        raise ValueError("Distribution is empty.")
    if any(v < 0 for v in dist.values()):
        raise ValueError("Distribution weights cannot be negative.")
    total = float(sum(dist.values()))
    if total <= 0:
        raise ValueError("Distribution must have positive weights.")
    return {k: float(v) / total for k, v in dist.items()}

# ------------------------------------------------------------------------------------------------------------------------------------
# CONFIGS
# ------------------------------------------------------------------------------------------------------------------------------------

DB_CONFIG: dict[str, Union[str, int]] = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASS", ""),
    "database": os.getenv("DB_NAME", "ecommerce_db_test"),
    "charset": "utf8mb4",
}

SEED: int = 42

# Nº de linhas (itens) por encomenda — distribuição simples e realista
# Interpretação: ~40% das encomendas têm 1 linha, 30% têm 2, etc.
CART_SIZE_DIST: dict[int, int] = {1: 40,2: 30, 3: 15, 4: 10, 5: 4, 6: 1}
CART_SIZE_WEIGHTS = normalize_distribution(CART_SIZE_DIST)

# Quantidade por linha (75% das linhas têm qty=1, 18% qty=2, etc.)
QTY_DIST: dict[int, int] = {1: 75, 2: 18, 3: 5, 4: 2}
QTY_WEIGHTS = normalize_distribution(QTY_DIST)

# Pesos por categoria (modulam prob. de seleção dos produtos)
CATEGORY_WEIGHTS: dict[str, float] = {
    'Electronics': 1.25,   
    'Fashion': 1.10,   
    'Home & Kitchen': 1.40,   
    'Beauty & Personal Care': 1.20,   
    'Sports & Fitness': 1.00,
    'Books': 0.90,
    'Toys': 0.95,
    'Gardening': 0.80,
    'Automotive': 1.05,
    'Pet Supplies': 1.10
}

# Para tornar o script idempotente: limpa os items das encomendas presentes (DELETE com JOIN)
# Se correres o script duas vezes, não vais duplicar dados em order_items, porque ele limpa antes de voltar a inserir.
CLEAR_EXISTING_ORDER_ITEMS: bool = True

# Batch size para inserir no MySQL
BATCH_SIZE: int = 20_000

# ------------------------------------------------------------------------------------------------------------------------------------
# DATABASE
# ------------------------------------------------------------------------------------------------------------------------------------

def get_connection() -> "mysql.connector.connection.MySQLConnection":
    """Abre ligação MySQL com as configs em DB_CONFIG."""
    return mysql.connector.connect(use_pure=True, **DB_CONFIG)

# ------------------------------------------------------------------------------------------------------------------------------------
# DATA MODEL
# ------------------------------------------------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class Product:
    product_id: int
    price: Decimal
    category_id: int

# ------------------------------------------------------------------------------------------------------------------------------------
# FETCHERS
# ------------------------------------------------------------------------------------------------------------------------------------

def fetch_orders(cur: "mysql.connector.cursor.MySQLCursor") -> list[int]:
    """Lê todos os order_id ordenados por data e id."""
    cur.execute("SELECT o.order_id FROM orders o ORDER BY o.order_date ASC, o.order_id ASC")
    rows = cur.fetchall()  # [(id,), ...]
    if not rows:
        raise RuntimeError("A tabela 'orders' está vazia — insere encomendas primeiro.")
    return [int(r[0]) for r in rows]


def fetch_products(cur: "mysql.connector.cursor.MySQLCursor") -> tuple[dict[int, Product], dict[int, str]]:
    """
    Lê produtos + categorias e devolve:
      - products: {product_id -> Product}
      - category_names: {category_id -> category_name}
    """
    cur.execute(
        """
        SELECT p.product_id, p.price, c.category_id, c.name
        FROM products p
        JOIN product_categories c ON p.category_id = c.category_id
        ORDER BY p.product_id
        """
    )
    rows = cur.fetchall()
    if not rows:
        raise RuntimeError("There are no products with associated category.")

    products: dict[int, Product] = {}
    category_names: dict[int, str] = {}
    for pid, price, cat_id, cat_name in rows:
        products[int(pid)] = Product(int(pid), Decimal(str(price)), int(cat_id))
        category_names[int(cat_id)] = str(cat_name)
    return products, category_names

# ------------------------------------------------------------------------------------------------------------------------------------
# PESOS / AMOSTRAGEM
# ------------------------------------------------------------------------------------------------------------------------------------

def product_weights(
    products: Mapping[int, Product],
    category_names: Mapping[int, str],
    default_weight: float = 1.0,
) -> dict[int, float]:
    """
    Mapeia product_id -> peso com base no nome da categoria e CATEGORY_WEIGHTS.
    """
    weights: dict[int, float] = {}
    for p in products.values():
        cat_name = category_names.get(p.category_id)
        w = CATEGORY_WEIGHTS.get(cat_name, default_weight)
        weights[p.product_id] = float(w)
    return weights


def choose_weighted_key(rng: random.Random, weights: Mapping[int, float]) -> int:
    """
    Escolhe uma chave (int) segundo pesos arbitrários (não precisam de normalizar).
    """
    if not weights:
        raise ValueError("Empty weights.")
    keys = list(weights.keys())
    vals = list(weights.values())
    return rng.choices(keys, weights=vals, k=1)[0]


def sample_unique_products_weighted(
    rng: random.Random,
    candidate_ids: Sequence[int],
    weights_map: Mapping[int, float],
    k: int,
) -> list[int]:
    """
    Amostra até k produtos distintos com pesos (sem reposição).
    Implementação O(k) com técnica swap-pop para evitar deslocações O(n).
    """
    if k <= 0 or not candidate_ids:
        return []

    # arrays locais (mutáveis)
    pool = list(candidate_ids)
    weights = [float(weights_map.get(pid, 1.0)) for pid in pool]

    k = min(k, len(pool))
    chosen: list[int] = []

    for _ in range(k):
        # escolher índice ponderado
        idx = rng.choices(range(len(pool)), weights=weights, k=1)[0]
        chosen.append(pool[idx])

        # swap com o último e pop (para O(1))
        last_index = len(pool) - 1
        pool[idx], pool[last_index] = pool[last_index], pool[idx]
        weights[idx], weights[last_index] = weights[last_index], weights[idx]

        pool.pop()
        weights.pop()

        # proteção (opcional): se os pesos remanescentes somarem 0, parar
        if weights and sum(weights) <= 0:
            break

    return chosen

# ------------------------------------------------------------------------------------------------------------------------------------
# PERSISTÊNCIA
# ------------------------------------------------------------------------------------------------------------------------------------

SQL_INSERT = """
INSERT INTO order_items (order_id, product_id, quantity, unit_price)
VALUES (%s, %s, %s, %s)
"""

def clear_existing_order_items(cur: "mysql.connector.cursor.MySQLCursor") -> None:
    """Limpa items de encomendas existentes (idempotência)."""
    cur.execute(
        """
        DELETE oi
        FROM order_items oi
        JOIN orders o ON o.order_id = oi.order_id
        """
    )

def insert_items_in_batches(
    conn: "mysql.connector.connection.MySQLConnection",
    rows: Sequence[tuple[int, int, int, Decimal]],
    batch_size: int,
) -> tuple[int, int]:
    """Insere rows por batches; devolve (total, num_batches)."""
    total, batches = 0, 0
    if not rows:
        return total, batches

    with conn.cursor() as cur:
        buf: list[tuple[int, int, int, Decimal]] = []
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
# ORQUESTRAÇÃO
# ------------------------------------------------------------------------------------------------------------------------------------

def run(seed: int = SEED) -> None:
    rng = random.Random(seed)
    print(
        f"🔌 A ligar à BD '{DB_CONFIG['database']}' como '{DB_CONFIG['user']}' em '{DB_CONFIG['host']}'..."
    )

    # ligação manual (para controlar rollback)
    conn = get_connection()
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            # fetch
            order_ids = fetch_orders(cur)
            products, category_names = fetch_products(cur)

            # pesos por produto
            weights_map = product_weights(products, category_names)

            # limpar items (idempotência)
            if CLEAR_EXISTING_ORDER_ITEMS:
                clear_existing_order_items(cur)
                conn.commit()

            # preparar buffers
            product_ids = list(products.keys())
            buffer: list[tuple[int, int, int, Decimal]] = []
            total_items = 0
            batches = 0

            start = time.perf_counter()

            # gerar items para cada encomenda
            for order_id in order_ids:
                # nº linhas desta encomenda
                cart_size = choose_weighted_key(rng, CART_SIZE_WEIGHTS)

                # amostra de produtos distintos
                chosen_products = sample_unique_products_weighted(
                    rng, product_ids, weights_map, cart_size
                )

                # criar linhas (qty ponderado + preço do produto)
                for pid in chosen_products:
                    qty = max(1, choose_weighted_key(rng, QTY_WEIGHTS))
                    price = products[pid].price
                    buffer.append((order_id, pid, qty, price))

                    # flush por batch
                    if BATCH_SIZE and len(buffer) >= BATCH_SIZE:
                        cur.executemany(SQL_INSERT, buffer)
                        conn.commit()
                        total_items += len(buffer)
                        batches += 1
                        buffer.clear()

            # flush final
            if buffer:
                cur.executemany(SQL_INSERT, buffer)
                conn.commit()
                total_items += len(buffer)
                batches += 1

            elapsed = time.perf_counter() - start
            avg_items = (total_items / len(order_ids)) if order_ids else 0.0

            print(f"✅ Inseridas {total_items} linhas em order_items para {len(order_ids)} encomendas em {batches} batch(es).")
            print(f"🧺 Linhas por encomenda (média): {avg_items:.2f}")
            if elapsed > 0:
                print(f"⏱️ Tempo de inserção: {elapsed:.2f}s (~{total_items/elapsed:.1f} rows/s)")

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
    run(SEED)

if __name__ == "__main__":
    main()