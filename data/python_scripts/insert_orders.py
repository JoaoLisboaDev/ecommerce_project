# -----------------------------------------------------------------------------------------------------------------------------------------------------
# DEPENDENCIES
# -----------------------------------------------------------------------------------------------------------------------------------------------------

import os 
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
import mysql.connector   
from typing import Mapping, Sequence, TypeVar, Union
from collections import Counter
from dotenv import load_dotenv                                          
load_dotenv() 

# Database Tables Dependencies
# -> customers: created and populated
# -> order_status: created and populated
# -> orders: created


# -----------------------------------------------------------------------------------------------------------------------------------------------------
# UTILS
# -----------------------------------------------------------------------------------------------------------------------------------------------------

K = TypeVar("K")

def normalize_distribution(dist: Mapping[K, Union[int, float]]) -> dict[K, float]:
    """
    Normaliza um dicionário {chave: peso} para somar 1.0.
    Recusa negativos e distribuição vazia / soma <= 0.
    """
    if not dist:
        raise ValueError("Distribution is empty.")
    if any(v < 0 for v in dist.values()):
        raise ValueError("Distribution weights cannot be negative.")
    total = float(sum(dist.values()))
    if total <= 0:
        raise ValueError("Distribution must have positive weights.")
    return {k: float(v) / total for k, v in dist.items()}

# ------------------------------------------------------------------------------------------------------------------------------------------------------
# CONFIGS
# -------------------------------------------------------------------------------------------------------------------------------------------------------

# Dicionário com parâmetros de conexão ao MySQL.
DB_CONFIG = {                                                                 
    "host": os.getenv("DB_HOST", "localhost"),                                
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER", "root"),                                     
    "password": os.getenv("DB_PASS", ""),                                     
    "database": os.getenv("DB_NAME", "ecommerce_db"),                      
    "charset": "utf8mb4"                                                      
}

# Semente fixa para tornar o aleatório reprodutível (mesmo output entre execuções).
SEED = 42

# Mínimo de encomendas pretendido (None para desativar)
MIN_TOTAL_ORDERS: int | None = 18_000

# Distribuição de nº de encomendas por cliente (em percentagem de clientes). Soma ~ 100.
# (ex.: 5% com 0 encomendas; 40% com 1; 25% com 2; 20% com 3; 10% com 4)
ORDERS_PER_CUSTOMER = {0: 5, 1: 40, 2: 30, 3: 20, 4: 10}
ORDERS_WEIGHTS = normalize_distribution(ORDERS_PER_CUSTOMER)

# Janela temporal para as encomendas (fechado à esquerda, aberto à direita)
ORDERS_START = datetime(2023, 1, 1)
ORDERS_END_EXCL = datetime(2025, 8, 1)  # exclusivo -> pára a 2025-07-31 23:59:59

# Pesos para estado final da encomenda
DELIVERED_WEIGHT = 85
CANCELLED_WEIGHT = 15

# Inserção por batches (para não encher buffers do connector/servidor)
BATCH_SIZE = 10_000

# ------------------------------------------------------------------------------------------------------------------------------------------------------------
# DATABASE
# ------------------------------------------------------------------------------------------------------------------------------------------------------------

def get_connection() -> "mysql.connector.connection.MySQLConnection":
    # Função que tem o objetivo abrir uma ligação ao MySQL usando as definições do dicionário DB_CONFIG.
    # Não recebe argumentos.
    # O tipo de retorno (annotation) indica que devolve um objeto de ligação MySQL (MySQLConnection).
    
    # Cria e devolve uma conexão ao MySQL.
    # mysql.connector.connect(...) é a função do conector oficial que estabelece ligação.
    # **DB_CONFIG faz o "unpacking" do dicionário de configuração (host, port, user, password, database, charset).
    return mysql.connector.connect(use_pure=True, **DB_CONFIG) 

# ------------------------------------------------------------------------------------------------------------------------------------------------------------
# FETCHERS
# ------------------------------------------------------------------------------------------------------------------------------------------------------------

def fetch_customer_ids(cur: "mysql.connector.cursor.MySQLCursor") -> list[int]:
    # Define a função fetch_customer_ids, que recebe um argumento cur (um cursor do MySQL).
    # Esta função vai buscar todos os IDs da tabela de clientes, confirma que a tabela não está vazia, e devolve a lista desses IDs já convertidos para inteiros.
    # Recebe como argumento um cursor MySQL já ligado à base de dados.
    # Retorna uma lista de inteiros (IDs de clientes)
    
    cur.execute("SELECT customer_id FROM customers ORDER BY customer_id ASC")
    # Com o cursor, é executada uma query SQL.
    
    rows = cur.fetchall()
    # O método fetchall() vai buscar todas as linhas devolvidas pela query
    # O resultado é uma lista de tuplos
    # rows = [(1,), (2,), (3,), ...]
    
    if not rows:
    # Se a lista estiver vazia, significa que não existem clientes na tabela   
        
        raise RuntimeError("Customers table is empty.")
        # Nesse caso, interrompe o programa lançando uma exceção
    
    return [int(r[0]) for r in rows]
    # Usa list comprehension para extrair o primeiro elemento de cada tuplo (r[0]).
    # Converte cada valor explicitamente para int (por segurança, caso venha como string ou Decimal).
    # Exemplo: [(1,), (2,), (3,)] → [1, 2, 3]



def fetch_customer_activation(cur: "mysql.connector.cursor.MySQLCursor") -> dict[int, datetime]:
    """
    Lê customer_id e created_at da tabela customers.
    """
    cur.execute("SELECT customer_id, created_at FROM customers ORDER BY customer_id ASC")
    rows = cur.fetchall()  # [(cid, created_at), ...]
    if not rows:
        raise RuntimeError("Customers table is empty.")
    # created_at já vem como datetime (timezone do servidor; estás a forçar '+00:00' no SET time_zone)
    return {int(cid): dt for cid, dt in rows}



def fetch_order_status_ids(cur: "mysql.connector.cursor.MySQLCursor") -> dict[str, int]:
    # Define a função fetch_order_status_ids, que recebe como argumento cur, um cursor do MySQL.
    
    cur.execute("SELECT code, order_status_id FROM order_status ORDER BY order_status_id ASC")
    # Com o cursor, é executada uma query SQL.
    
    mapping: dict[str, int] = {code: int(sid) for code, sid in cur.fetchall()}
    # fetchall() devolve uma lista de tuplos, ex.: [("delivered", 1), ("cancelled", 2), ("pending", 3)]
    # A compreensão de dicionário cria um mapping code -> id (convertendo sid para int).
    # Exemplo: {"delivered": 1, "cancelled": 2, "pending": 3}
    
    missing = [c for c in ("delivered", "cancelled") if c not in mapping]
    # Verifica se os estados essenciais ("delivered" e "cancelled") existem no dicionário.
    # Se algum não existir, ele aparece na lista missing.
    
    if missing:
    # Se a lista missing não estiver vazia…

        raise RuntimeError(f"Faltam códigos na tabela 'order_status': {missing}")
        # …levanta uma exceção RuntimeError indicando quais códigos estão em falta.
    
    return mapping
    # Caso tudo esteja ok, devolve o dicionário com os códigos de estado e respetivos IDs.

# -----------------------------------------------------------------------------------------------------------------------------------------------------
# HELPERS (probabilities, times)
# -----------------------------------------------------------------------------------------------------------------------------------------------------

def build_customer_quotas(
    customer_ids: Sequence[int],
    buckets: Sequence[int],
    probs: Sequence[float],
    rng: random.Random,
) -> dict[int, int]:
    """
    Atribui a cada cliente um nº de encomendas, segundo distribuição discreta.
    `buckets` = possíveis nºs (ex.: [0,1,2,3,4]); `probs` = pesos normalizados (mesmo comprimento).
    """
    if not customer_ids:
        return {}
    if not buckets or not probs or len(buckets) != len(probs):
        raise ValueError("Buckets and probs must be non-empty and of identical length.")
    return {cid: rng.choices(buckets, weights=probs, k=1)[0] for cid in customer_ids}


def quotas_to_orders(quotas: Mapping[int, int]) -> list[int]:
    # Define a função quotas_to_orders.
    # Parâmetro quotas: dicionário (quotas) no formato {customer_id: nº_encomendas}.
    # Output: lista de inteiros (IDs de cliente), onde cada cliente aparece repetido q vezes.

    return [cid for cid, q in quotas.items() for _ in range(q)]
    # Usa uma list comprehension com dois loops embutidos:
    # 1º loop → percorre cada par (cid, q) do dicionário quotas.
    #     - cid é o ID do cliente
    #     - q é o número de encomendas atribuídas a esse cliente
    # 2º loop → "for _ in range(q)" repete o cliente q vezes.
    # Para cada repetição, devolve o cid.
    # O resultado final é uma lista com um elemento por encomenda.
    # Exemplo: {1: 2, 2: 3} → [1, 1, 2, 2, 2]


def random_order_datetime(rng: random.Random, start: datetime, end_excl: datetime) -> datetime:
    """Sazonalidade simples: pesos por mês + boost ao fim-de-semana (+ opcional Black Friday)."""
    if start >= end_excl:
        raise ValueError("`start` must be earlier than `end_excl`.")

    # 1) Pesos por mês
    month_weight = {
        1:0.95, 2:0.95, 3:1.00, 4:1.05, 5:1.10,
        6:0.95, 7:0.85, 8:0.90, 9:1.10, 10:1.25,
        11:1.45, 12:1.80
    }

    # 2) Escolhe mês ponderado
    from calendar import monthrange
    months, weights = [], []
    cur = datetime(start.year, start.month, 1)
    end_anchor = datetime(end_excl.year, end_excl.month, 1)
    while cur <= end_anchor:
        m_start = max(cur, start)
        last_day = monthrange(cur.year, cur.month)[1]
        m_end_excl = min(datetime(cur.year, cur.month, last_day, 23, 59, 59) + timedelta(seconds=1), end_excl)
        if m_start < m_end_excl:
            months.append((m_start, m_end_excl))
            weights.append(month_weight.get(cur.month, 1.0))
        cur = datetime(cur.year+1, 1, 1) if cur.month == 12 else datetime(cur.year, cur.month+1, 1)
    probs = [w/sum(weights) for w in weights]
    m_start, m_end_excl = random.choices(months, weights=probs, k=1)[0]

    # 3) Dia ponderado (fins-de-semana / eventos)
    days = []
    d = m_start.date()
    last = (m_end_excl - timedelta(seconds=1)).date()
    while d <= last:
        w = 1.0
        if d.weekday() >= 5: w *= 1.25           # Sábado/Domingo
        if d.month == 11 and d.weekday() == 4 and 22 <= d.day <= 28: w *= 2.5  # Black Friday
        if d.month == 12 and 20 <= d.day <= 24: w *= 1.5                       # Natal
        days.append((d, w))
        d += timedelta(days=1)
    probs = [w/sum(w for _, w in days) for _, w in days]
    chosen_day = random.choices([d for d, _ in days], weights=probs, k=1)[0]

    # 4) Hora simples (pico 17–21h)
    hour_weights = [1]*24
    for h in range(17, 22): hour_weights[h] = 2
    h = random.choices(range(24), weights=hour_weights, k=1)[0]
    m = rng.randint(0, 59)
    s = rng.randint(0, 59)
    return datetime(chosen_day.year, chosen_day.month, chosen_day.day, h, m, s)

# -----------------------------------------------------------------------------------------------------------------------------------------------------
# DATA MODEL
# -----------------------------------------------------------------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class OrderRow:
    customer_id: int
    order_date: datetime
    order_status_id: int
    created_at: datetime
    updated_at: datetime

# -----------------------------------------------------------------------------------------------------------------------------------------------------
# GENERATION
# -----------------------------------------------------------------------------------------------------------------------------------------------------

def plan_orders_for_customers(
    customer_ids: Sequence[int],
    orders_weights: Mapping[int, float],
    rng: random.Random,
    min_total_orders: int | None,
) -> list[int]:
    """
    Gera o plano de encomendas (lista de customer_ids, um por encomenda),
    respeitando um mínimo global se fornecido.
    """
    buckets = list(orders_weights.keys())
    probs = list(orders_weights.values())

    attempt = 0
    while True:
        attempt += 1
        quotas = build_customer_quotas(customer_ids, buckets, probs, rng)
        plan = quotas_to_orders(quotas)

        if min_total_orders is None or len(plan) >= min_total_orders:
            rng.shuffle(plan)
            if attempt > 1:
                print(f"[info] mínimo atingido na tentativa {attempt}: total={len(plan)}")
            return plan

        print(f"[info] tentativa {attempt}: total={len(plan)} < mínimo={min_total_orders} → regenerar...")


def generate_order_rows(
    customer_plan: Sequence[int],
    status_map: Mapping[str, int],
    rng: random.Random,
    start: datetime,
    end_excl: datetime,
    delivered_weight: int,
    cancelled_weight: int,
    *,
    activation_map: Mapping[int, datetime],
) -> list[OrderRow]:
    """
    Para cada customer_id no plano, gera um order_date ∈ [max(start, activation[cid]), end_excl),
    ordena cronologicamente e constrói OrderRow. Clientes ainda não "ativos" na janela são ignorados.
    """
    if not customer_plan:
        return []

    status_choices = ("delivered", "cancelled")
    status_weights = (delivered_weight, cancelled_weight)

    pairs: list[tuple[datetime, int]] = []
    for cid in customer_plan:
        # limite inferior: quando o cliente "existe"
        cust_start = max(start, activation_map.get(cid, start))
        if cust_start >= end_excl:
            # este cliente só ficou ativo depois da janela — ignora esta ocorrência
            continue

        dt = random_order_datetime(rng, cust_start, end_excl)
        pairs.append((dt, cid))

    if not pairs:
        return []

    # ordenar por data para manter cronologia global
    pairs.sort(key=lambda t: t[0])

    rows: list[OrderRow] = []
    for dt, cust_id in pairs:
        status_code = rng.choices(status_choices, weights=status_weights, k=1)[0]
        status_id = status_map[status_code]
        rows.append(OrderRow(cust_id, dt, status_id, dt, dt))
    return rows

# -----------------------------------------------------------------------------------------------------------------------------------------------------
# PERSISTÊNCIA
# -----------------------------------------------------------------------------------------------------------------------------------------------------

SQL_INSERT = """
INSERT INTO orders (customer_id, order_date, order_status_id, created_at, updated_at)
VALUES (%s, %s, %s, %s, %s)
"""

def insert_orders_in_batches(
    conn: "mysql.connector.connection.MySQLConnection",
    rows: Sequence[OrderRow],
    batch_size: int,
) -> tuple[int, int]:
    """
    Insere `rows` por batches; devolve (total_inserted, num_batches).
    """
    total_inserted = 0
    batches = 0

    if not rows:
        return 0, 0

    with conn.cursor() as cur:
        buffer: list[tuple[int, datetime, int, datetime, datetime]] = []

        for r in rows:
            buffer.append((r.customer_id, r.order_date, r.order_status_id, r.created_at, r.updated_at))
            if batch_size and len(buffer) >= batch_size:
                cur.executemany(SQL_INSERT, buffer)
                conn.commit()
                total_inserted += len(buffer)
                batches += 1
                buffer.clear()

        if buffer:
            cur.executemany(SQL_INSERT, buffer)
            conn.commit()
            total_inserted += len(buffer)
            batches += 1

    return total_inserted, batches

# -----------------------------------------------------------------------------------------------------------------------------------------------------
# RELATÓRIOS
# -----------------------------------------------------------------------------------------------------------------------------------------------------

def report_summary(rows: Sequence[OrderRow], status_map: Mapping[str, int]) -> None:
    """Imprime sumário por ano, por estado e distribuição observada por cliente."""
    if not rows:
        print("⚠️ Nenhuma encomenda gerada — nada a reportar.")
        return

    # por ano
    year_counts: Counter[int] = Counter(r.order_date.year for r in rows)
    print("📅 Distribuição por ano:")
    for y in (2023, 2024, 2025):
        print(f"  - {y}: {year_counts.get(y, 0)}")

    # por estado
    inv_status = {v: k for k, v in status_map.items()}
    status_counts: Counter[str] = Counter(inv_status[r.order_status_id] for r in rows)
    print("🚚 Estados:")
    for s in ("delivered", "cancelled"):
        print(f"  - {s}: {status_counts.get(s, 0)}")

    # distribuição observada por nº de encomendas / cliente
    per_customer: Counter[int] = Counter(r.customer_id for r in rows)
    bucket_obs: Counter[int] = Counter(per_customer.values())
    n_clients_observed = len({r.customer_id for r in rows})
    print("👥 Clientes por nº de encomendas (observado):")
    for k in sorted(bucket_obs):
        pct = 100.0 * bucket_obs[k] / n_clients_observed if n_clients_observed else 0.0
        print(f"  {k}: {bucket_obs[k]} clientes ({pct:.2f}%)")

# -----------------------------------------------------------------------------------------------------------------------------------------------------
# ORQUESTRAÇÃO
# -----------------------------------------------------------------------------------------------------------------------------------------------------

def max_orders_possible(n_clients: int, dist_keys: Sequence[int]) -> int:
    """Limite superior teórico = n_clients * max(buckets)."""
    return n_clients * (max(dist_keys) if dist_keys else 0)


def run(seed: int = SEED) -> None:
    rng = random.Random(seed)
    print(f"🔌 A ligar à BD '{DB_CONFIG['database']}' como '{DB_CONFIG['user']}' em '{DB_CONFIG['host']}'...")

    # Coneção e transação
    conn = get_connection()
    conn.autocommit = False
    
    try:
        with conn.cursor() as cur:
            
            cur.execute("SET time_zone = '+00:00';")
            
            # Fetchers
            customer_ids = fetch_customer_ids(cur)
            activation_map = fetch_customer_activation(cur)
            status_map = fetch_order_status_ids(cur)
            
        # Só clientes com activation < ORDERS_END_EXCL
        eligible_customers = [cid for cid in customer_ids if activation_map.get(cid, ORDERS_START) < ORDERS_END_EXCL]
        if not eligible_customers:
            raise RuntimeError("No available customers at the order creation window.")
        
        # hard max com base apenas nos elegíveis
        hard_max = max_orders_possible(len(eligible_customers), list(ORDERS_WEIGHTS.keys()))
        if MIN_TOTAL_ORDERS is not None and MIN_TOTAL_ORDERS > hard_max:
            raise RuntimeError(
                f"MIN_TOTAL_ORDERS={MIN_TOTAL_ORDERS} is impossible with {len(eligible_customers)} available customers."
                f"and max {max(ORDERS_WEIGHTS.keys())} orders/customers (theoric max = {hard_max})."
            )
        
        # Plano (lista de customer_ids; 1 por encomenda)
        customer_plan = plan_orders_for_customers(eligible_customers, ORDERS_WEIGHTS, rng, MIN_TOTAL_ORDERS)
        if not customer_plan:
            print("⚠️ Quotas resulted in 0 orders - nothing to insert.")
            return

        # gerar linhas respeitando activation por cliente
        rows = generate_order_rows(
            customer_plan,
            status_map,
            rng,
            ORDERS_START,
            ORDERS_END_EXCL,
            DELIVERED_WEIGHT,
            CANCELLED_WEIGHT,
            activation_map=activation_map,
        )

        # inserir em batches
        start_time = time.perf_counter()
        total_inserted, batches = insert_orders_in_batches(conn, rows, BATCH_SIZE)
        elapsed = time.perf_counter() - start_time

        print(f"✅ Inserted {total_inserted} orders in {batches} batch(es).")
        if elapsed > 0:
            print(f"⏱️ Insertion time: {elapsed:.2f}s (~{total_inserted/elapsed:.1f} rows/s)")

        # relatórios
        report_summary(rows, status_map)

    except mysql.connector.Error as e:
        conn.rollback()
        print(f"[MySQL] {e.__class__.__name__}: {e}")
        return
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()

# -----------------------------------------------------------------------------------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------------------------------------------------------------------------------

def main() -> None:
    run(SEED)

if __name__ == "__main__":
    main()