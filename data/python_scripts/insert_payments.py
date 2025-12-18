# ------------------------------------------------------------------------------------------------------------------------------------
# DEPENDENCIES
# ------------------------------------------------------------------------------------------------------------------------------------

import os
import time
import random
from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping, Sequence, TypeVar, Union

import mysql.connector
from datetime import datetime, timedelta
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

def weighted_choice_key(rng: random.Random, weights: Mapping[K, float]) -> K:
    """
    Escolhe uma chave segundo pesos arbitrários (não precisam normalizar).
    """
    if not weights:
        raise ValueError("Empty weights.")
    keys = list(weights.keys())
    vals = list(weights.values())
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

SEED = 42

# Teto global de tentativas por encomenda
MAX_GLOBAL_ATTEMPTS = 4

# Dist. do nº total de tentativas por encomenda (capada por MAX_GLOBAL_ATTEMPTS)
GLOBAL_ATTEMPT_DIST = {1: 45, 2: 32, 3: 18, 4: 5}
GLOBAL_ATTEMPT_WEIGHTS = normalize_distribution(GLOBAL_ATTEMPT_DIST)

# Janela máx. (exclusiva) depois da compra para *todos* os registos de pagamento
PAYMENT_WINDOW_SECONDS = 48 * 60 * 60   # 2 dias

# Config de métodos de pagamento
PAYMENT_METHOD_CONFIG: dict[str, dict[str, Union[int, float]]] = {
    "card":         {"weight": 0.58, "max_attempts": 3, "stay_with_method_prob": 0.68, "success_rate": 0.62},
    "paypal":       {"weight": 0.18, "max_attempts": 3, "stay_with_method_prob": 0.55, "success_rate": 0.56},
    "mbway":        {"weight": 0.18, "max_attempts": 3, "stay_with_method_prob": 0.60, "success_rate": 0.50},
    "bank_transfer":{"weight": 0.06, "max_attempts": 2, "stay_with_method_prob": 0.35, "success_rate": 0.35},
}

# Idempotência: limpar pagamentos antes de inserir
CLEAR_EXISTING_PAYMENTS = True

# Batch size
BATCH_SIZE = 20_000

# ------------------------------------------------------------------------------------------------------------------------------------
# DATABASE
# ------------------------------------------------------------------------------------------------------------------------------------

def get_connection() -> "mysql.connector.connection.MySQLConnection":
    """Abre ligação MySQL com as configs em DB_CONFIG."""
    return mysql.connector.connect(use_pure=True, **DB_CONFIG)

# ------------------------------------------------------------------------------------------------------------------------------------
# DATA MODELS
# ------------------------------------------------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class OrderInfo:
    order_id: int
    order_date: datetime
    order_status: int
    order_total: Decimal

# ------------------------------------------------------------------------------------------------------------------------------------
# FETCHERS
# ------------------------------------------------------------------------------------------------------------------------------------

def fetch_orders_with_totals(cur: "mysql.connector.cursor.MySQLCursor") -> list[OrderInfo]:
    """
    Lê orders + total calculado; filtra total > 0.
    """
    cur.execute(
        """
        SELECT 
            o.order_id,
            o.order_date,
            o.order_status_id,
            COALESCE(SUM(oi.quantity * oi.unit_price), 0) AS total
        FROM orders o
        LEFT JOIN order_items oi ON oi.order_id = o.order_id
        GROUP BY o.order_id, o.order_date, o.order_status_id
        HAVING total > 0
        ORDER BY o.order_id ASC
        """
    )
    rows = cur.fetchall()
    if not rows:
        raise RuntimeError("The 'orders' table has no paid-total candidates (total > 0).")
    out: list[OrderInfo] = []
    for oid, dt, sid, total in rows:
        out.append(OrderInfo(int(oid), dt, int(sid), Decimal(str(total))))
    return out


def fetch_payment_methods(cur: "mysql.connector.cursor.MySQLCursor") -> tuple[dict[int, str], dict[str, int]]:
    cur.execute("SELECT payment_method_id, code FROM payment_methods ORDER BY payment_method_id ASC")
    rows = cur.fetchall()
    if not rows:
        raise RuntimeError("The 'payment_methods' table is empty.")
    id_to_code = {int(i): str(c) for i, c in rows}
    code_to_id = {c: i for i, c in id_to_code.items()}
    return id_to_code, code_to_id


def fetch_payment_statuses(cur: "mysql.connector.cursor.MySQLCursor") -> tuple[dict[int, str], dict[str, int]]:
    cur.execute("SELECT payment_status_id, code FROM payment_status ORDER BY payment_status_id ASC")
    rows = cur.fetchall()
    if not rows:
        raise RuntimeError("The 'payment_status' table is empty.")
    id_to_code = {int(i): str(c) for i, c in rows}
    # usar lowercase nas chaves de lookup
    code_to_id = {c.lower(): i for i, c in id_to_code.items()}
    return id_to_code, code_to_id

# ------------------------------------------------------------------------------------------------------------------------------------
# LIMPEZA (IDEMPOTÊNCIA)
# ------------------------------------------------------------------------------------------------------------------------------------

def clear_existing_payments(cur: "mysql.connector.cursor.MySQLCursor") -> None:
    """
    Remove payments associados a encomendas (para evitar duplicação ao re-correr).
    """
    cur.execute(
        """
        DELETE p
        FROM payments p
        JOIN (
            SELECT 
                o.order_id,
                COALESCE(SUM(oi.quantity * oi.unit_price), 0) AS total
            FROM orders o
            LEFT JOIN order_items oi ON oi.order_id = o.order_id
            GROUP BY o.order_id
            HAVING total > 0
        ) t ON p.order_id = t.order_id
        """
    )

# ------------------------------------------------------------------------------------------------------------------------------------
# LÓGICA DE GERAÇÃO
# ------------------------------------------------------------------------------------------------------------------------------------

def build_method_weights() -> dict[str, float]:
    """Extrai os pesos 'weight' por método, default 0.0 se em falta."""
    return {m: float(cfg.get("weight", 0.0)) for m, cfg in PAYMENT_METHOD_CONFIG.items()}

def available_methods_by_cap(method_counts: Mapping[str, int]) -> list[str]:
    """Filtra métodos que ainda não atingiram o seu max_attempts (respeitando também MAX_GLOBAL_ATTEMPTS)."""
    out: list[str] = []
    for method, cfg in PAYMENT_METHOD_CONFIG.items():
        max_m = int(cfg.get("max_attempts", MAX_GLOBAL_ATTEMPTS))
        if method_counts.get(method, 0) < min(max_m, MAX_GLOBAL_ATTEMPTS):
            out.append(method)
    return out

def pick_next_method(rng: random.Random, current: str, method_counts: Mapping[str, int]) -> str:
    """
    Decide manter ou trocar método.
    - Mantém com prob. stay_with_method_prob (se não atingiu cap).
    - Caso contrário, escolhe outro ponderado por weight, excluindo métodos no limite.
    """
    cfg = PAYMENT_METHOD_CONFIG.get(current, {})
    stay_prob = float(cfg.get("stay_with_method_prob", 1.0))
    max_curr = int(cfg.get("max_attempts", MAX_GLOBAL_ATTEMPTS))
    if method_counts.get(current, 0) >= min(max_curr, MAX_GLOBAL_ATTEMPTS):
        stay = False
    else:
        stay = rng.random() <= stay_prob

    if stay:
        return current

    available = available_methods_by_cap(method_counts)
    if current in available:
        available.remove(current)
    if not available:
        return current  # sem alternativas viáveis

    weights = {m: float(PAYMENT_METHOD_CONFIG[m].get("weight", 0.0)) for m in available}
    return weighted_choice_key(rng, weights)

def sorted_attempt_times(rng: random.Random, base_dt: datetime, n_attempts: int) -> list[datetime]:
    """
    Gera n timestamps aleatórios em (0, PAYMENT_WINDOW_SECONDS], ordenados.
    """
    if n_attempts <= 0:
        return []
    max_seconds = max(1, PAYMENT_WINDOW_SECONDS - 2)
    n_attempts = min(n_attempts, max_seconds)

    chosen: set[int] = set()
    while len(chosen) < n_attempts:
        chosen.add(rng.randint(1, max_seconds))

    offsets = sorted(chosen)
    return [base_dt + timedelta(seconds=s) for s in offsets]

def draw_total_attempts_planned(rng: random.Random) -> int:
    n = weighted_choice_key(rng, GLOBAL_ATTEMPT_WEIGHTS)
    return min(int(n), MAX_GLOBAL_ATTEMPTS)

# ------------------------------------------------------------------------------------------------------------------------------------
# PERSISTÊNCIA
# ------------------------------------------------------------------------------------------------------------------------------------

SQL_INSERT_PAYMENT = """
INSERT INTO payments (order_id, attempt_no, payment_date, amount_paid, payment_method_id, payment_status_id)
VALUES (%s, %s, %s, %s, %s, %s)
"""

def insert_payments_in_batches(
    conn: "mysql.connector.connection.MySQLConnection",
    rows: Sequence[tuple[int, int, datetime, Decimal, int, int]],
    batch_size: int,
) -> tuple[int, int]:
    """Insere pagamentos por batches; devolve (total, num_batches)."""
    total, batches = 0, 0
    if not rows:
        return total, batches

    with conn.cursor() as cur:
        buf: list[tuple[int, int, datetime, Decimal, int, int]] = []
        for rec in rows:
            buf.append(rec)
            if batch_size and len(buf) >= batch_size:
                cur.executemany(SQL_INSERT_PAYMENT, buf)
                conn.commit()
                total += len(buf)
                batches += 1
                buf.clear()

        if buf:
            cur.executemany(SQL_INSERT_PAYMENT, buf)
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

    conn = get_connection()
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            # fetch
            _, method_id_by_code = fetch_payment_methods(cur)
            _, status_id_by_code = fetch_payment_statuses(cur)
            orders = fetch_orders_with_totals(cur)

            # limpeza (idempotência)
            if CLEAR_EXISTING_PAYMENTS:
                clear_existing_payments(cur)
                conn.commit()

            # pesos por método
            method_weights = build_method_weights()

            rows: list[tuple[int, int, datetime, Decimal, int, int]] = []

            for order in orders:
                planned_attempts = draw_total_attempts_planned(rng)
                attempt_times = sorted_attempt_times(rng, order.order_date, planned_attempts)

                current_method = weighted_choice_key(rng, method_weights)
                method_counts: dict[str, int] = {m: 0 for m in PAYMENT_METHOD_CONFIG}

                paid = False
                attempt_no = 0

                for t in attempt_times:
                    if paid:
                        break

                    if attempt_no == 0:
                        method = current_method
                    else:
                        method = pick_next_method(rng, current_method, method_counts)
                        current_method = method

                    # cap por método
                    max_m = int(PAYMENT_METHOD_CONFIG[method].get("max_attempts", MAX_GLOBAL_ATTEMPTS))
                    if method_counts[method] >= min(max_m, MAX_GLOBAL_ATTEMPTS):
                        alt_avail = available_methods_by_cap(method_counts)
                        if method in alt_avail:
                            alt_avail.remove(method)
                        if alt_avail:
                            alt_weights = {m: float(PAYMENT_METHOD_CONFIG[m].get("weight", 0.0)) for m in alt_avail}
                            method = weighted_choice_key(rng, alt_weights)
                            current_method = method
                        else:
                            break  # sem alternativas dentro do cap

                    method_counts[method] = method_counts.get(method, 0) + 1
                    attempt_no += 1

                    success_rate = float(PAYMENT_METHOD_CONFIG[method].get("success_rate", 0.0))
                    is_success = rng.random() < success_rate

                    method_id = method_id_by_code.get(method)
                    if method_id is None:
                        # método desconhecido na BD -> ignora tentativa
                        continue

                    if is_success:
                        rows.append(
                            (
                                order.order_id,
                                attempt_no,
                                t,
                                order.order_total,
                                method_id,
                                status_id_by_code["paid"],
                            )
                        )
                        paid = True
                        break
                    else:
                        rows.append(
                            (
                                order.order_id,
                                attempt_no,
                                t,
                                Decimal("0.00"),
                                method_id,
                                status_id_by_code["failed"],
                            )
                        )
                
                if not paid:
                    if order.order_status != 5:
                        # Força sucesso final para encomendas não-canceladas
                        t_force = (
                            (attempt_times[-1] + timedelta(seconds=1))
                            if attempt_times else (order.order_date + timedelta(seconds=1))
                        )

                        # Escolhe um método válido (tenta o corrente; se não houver, usa o primeiro da tabela)
                        method_id = method_id_by_code.get(current_method)
                        if method_id is None:
                            method_id = next(iter(method_id_by_code.values()))

                        attempt_no += 1
                        rows.append(
                            (
                                order.order_id,
                                attempt_no,
                                t_force,
                                order.order_total,
                                method_id,
                                status_id_by_code["paid"],
                            )
                        )
                    else:
                        # order_status == 5 (cancelada): é aceitável terminar com falha
                        pass
            
            
            
            start = time.perf_counter()
            total, batches = insert_payments_in_batches(conn, rows, BATCH_SIZE)
            elapsed = time.perf_counter() - start

            print(f"✅ Inseridos {total} registos de pagamento para {len(orders)} encomendas em {batches} batch(es).")
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
    run(SEED)

if __name__ == "__main__":
    main()
