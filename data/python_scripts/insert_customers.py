# -------------------------------------------------------------------------------------------------------------------------------
# DEPENDENCIES
# -------------------------------------------------------------------------------------------------------------------------------

import os                                                                  
import random
from typing import Sequence, Iterable, Iterator, TypeVar, Mapping                                                              
from faker import Faker                                                   
import mysql.connector                                                     
from dataclasses import dataclass
from datetime import date, datetime, timedelta                            
import calendar                                                               
import time
import re
import unicodedata                                                                
from dotenv import load_dotenv                                               
load_dotenv()   

# Database Table Dependencies  
# -> countries (criada e populada)
# -> customers (criada)                                                  

# -----------------------------------------------------------------------------------------------------------------------------------
# UTILS
# -----------------------------------------------------------------------------------------------------------------------------------

K = TypeVar("K")

def normalize_distribution(dist: Mapping[K, float], *, round_to: int | None = None) -> dict[K, float]:
    if not dist:
        raise ValueError("Distribution is empty.")
    if any(v < 0 for v in dist.values()):
        raise ValueError("Distribution weights cannot be negative.")
    total = sum(dist.values())
    if total <= 0:
        raise ValueError("Distribution must have positive weights.")

    normalized = {k: v / total for k, v in dist.items()}
    
    if round_to is not None:
        normalized = {k: round(v, round_to) for k, v in normalized.items()}

    return normalized

# -----------------------------------------------------------------------------------------------------------------------------------
# CONFIGS
# -----------------------------------------------------------------------------------------------------------------------------------

# Dicionário com parâmetros de conexão ao MySQL.
DB_CONFIG = {                                                                 
    "host": os.getenv("DB_HOST", "localhost"),                                
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER", "root"),                                     
    "password": os.getenv("DB_PASS", ""),                                     
    "database": os.getenv("DB_NAME", "ecommerce_db"),                      
    "charset": "utf8mb4"                                                      
}

# Número total de clientes a gerar.
N_CUSTOMERS = 10_000                                                          

# Semente fixa para tornar geração de dados reprodutível.
SEED = 42                                                                     

# Número de clientes a inserir por batch no MySQL.
BATCH_SIZE = 1000                                                             

# Percentagem de clientes por país (soma = 100).
COUNTRY_DISTRIBUTION = {                                                      
    "Portugal": 25,
    "Spain": 15,
    "France": 12,
    "Germany": 10,
    "Italy": 10,
    "Netherlands": 6,
    "Belgium": 4,
    "Greece": 4,
    "Croatia": 4,
    "Ireland": 10
}

# Time window for the customer creation date
CUSTOMERS_START = datetime(2023, 1, 1, 0, 0, 0)
CUSTOMERS_END_EXCL = datetime(2025, 8, 1, 0, 0, 0)  # exclusive

# Distribuição etária em percentagens.
AGE_GROUPS = {                                                                
    (18, 29): 40,   # 40% between 18 and 30
    (30, 64): 50,   # 50% between 30 and 65
    (65, 80): 10,   # 10% between 65 and 80
}

COUNTRY_WEIGHTS = normalize_distribution(COUNTRY_DISTRIBUTION)
AGE_WEIGHTS = normalize_distribution(AGE_GROUPS)

# Lista de cidades para cada país.
CITIES_BY_COUNTRY = {                                                                                                                                            
    "Portugal": ["Lisboa", "Porto", "Braga", "Coimbra", "Faro", "Aveiro", "Guimarães", "Évora", "Leiria", "Setúbal"],
    "Spain": ["Madrid", "Barcelona", "Valencia", "Seville", "Bilbao","Malaga", "Zaragoza", "Murcia", "Palma de Mallorca", "Valladolid"],
    "France": ["Paris", "Marseille", "Lyon", "Toulouse", "Nice","Nantes", "Strasbourg", "Montpellier", "Bordeaux", "Lille"],
    "Germany": ["Berlin", "Munich", "Hamburg", "Cologne", "Frankfurt","Stuttgart", "Dusseldorf", "Dresden", "Leipzig", "Hanover"],
    "Italy": ["Rome", "Milan", "Naples", "Turin", "Bologna","Florence", "Genoa", "Venice", "Verona", "Palermo"],
    "Ireland": ["Dublin", "Cork", "Limerick", "Galway", "Waterford","Kilkenny", "Sligo", "Wexford", "Athlone", "Drogheda"],
    "Netherlands": ["Amsterdam", "Rotterdam", "The Hague", "Utrecht", "Eindhoven","Tilburg", "Groningen", "Breda", "Nijmegen", "Maastricht"],
    "Belgium": ["Brussels", "Antwerp", "Ghent", "Liege", "Bruges","Namur", "Leuven", "Mons", "Mechelen", "Ostend"],
    "Greece": ["Athens", "Thessaloniki", "Patras", "Heraklion", "Larissa","Volos", "Ioannina", "Kavala", "Kalamata", "Chania"],
    "Croatia": ["Zagreb", "Split", "Rijeka", "Osijek", "Zadar","Pula", "Dubrovnik", "Slavonski Brod", "Karlovac", "Varazdin"],
}

EMAIL_DOMAIN_DISTRIBUTION = {
    'gmail.com': 40,
    'yahoo.com': 30,
    'hotmail.com': 22,
    'outlook.com': 8
}

# Locale usado pelo Faker para gerar nomes realistas por país.
# A Bélgica tem duas línguas principais (fr/nl). Por simplicidade foi escolhido fr_BE.
FAKER_LOCALE_BY_COUNTRY = {                                                                             
    "Portugal": "pt_PT", 
    "Spain": "es_ES", 
    "France": "fr_FR", 
    "Germany": "de_DE",
    "Italy": "it_IT", 
    "Netherlands": "nl_NL", 
    "Belgium": "fr_BE",
    "Greece": "el_GR", 
    "Croatia": "hr_HR", 
    "Ireland": "en_IE",
}

SQL_INSERT = """
INSERT INTO customers (first_name, last_name, email, birth_date, city, country_id, created_at, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

# --------------------------------------------------------------------------------------------------------------------------------------
# DATABASE
# --------------------------------------------------------------------------------------------------------------------------------------

def get_connection() -> "mysql.connector.connection.MySQLConnection":
    return mysql.connector.connect(use_pure=True, **DB_CONFIG)



# -----------------------------------------------------------------------------------------------------------------------------------------
# DATA HELPERS
# -----------------------------------------------------------------------------------------------------------------------------------------

# Tenta mudar apenas o ano de uma data
def safe_shift_year(dt: date, year: int) -> date:                                                       
    """
    Tenta alterar apenas o ano de uma data, preservando o mês e o dia sempre que possível.

    Args:
        dt: Data original.
        year: Novo ano desejado.

    Returns:
        Uma nova data com o mesmo mês e dia, mas com o ano alterado.

    Notes:
        Se a data original for 29 de fevereiro e o novo ano não for bissexto,
        devolve o dia 28 de fevereiro.
    """
    
    try: 
        # Tenta substituir o ano da data que passamos no parâmetro dt pelo ano que passamos no parâmetro year. O mês e dia são mantidos.
        # Se funcionar, devolve a mesma data, mas com o novo ano
        return dt.replace(year=year) 
    
    # Se a data for inválida (ex: 29-Fev em ano não bissexto)
    # Apenas pode acontecer se a data do parâmetro dt for 29/02 e o ano para o qual queremos trocar seja não bissexto.                                                             
    except ValueError: 
        
        # Usa o dia anterior (28-Fev)
        # Se dt = date(2024, 2, 29) e year = 2023, o resultado final será 2023-02-28.                                                                              
        return (dt - timedelta(days=1)).replace(year=year)                                              



# Gera data de nascimento aleatória dentro do intervalo
# Exp: random_birthdate(30, 65)
# o resultado é dependente do dia em que corre (usa date.today()), o que significa que voltas a obter outras datas se correres noutro dia (mesmo com a mesma semente).
def random_birthdate(age_min: int, age_max: int) -> date:                                               
    """
    Gera uma data de nascimento aleatória dentro de uma faixa etária.

    Args:
        age_min: Idade mínima (inclusive).
        age_max: Idade máxima (inclusive).

    Returns:
        Uma data de nascimento aleatória compatível com a faixa etária.

    Notes:
        O resultado depende da data atual (`date.today()`), portanto,
        diferentes execuções podem gerar datas distintas mesmo com a mesma semente.
    """
    
    # Obtém a data de hoje
    # Exp: 2025-09-29
    today = date.today()
    
    # Data de nascimento mais antiga possível
    # Exp: min_birth = safe_shift_year('2025-09-29', 1960) = 1960-09-29                                                                    
    min_birth = safe_shift_year(today, today.year - age_max)
    
    # Data de nascimento mais recente possível  
    # EXP: max_birth = safe_shift_year('2025-09-29', 1995) = 1995-09-29                                
    max_birth = safe_shift_year(today, today.year - age_min)
    
    # Número total de dias válidos entre limites                                  
    span = (max_birth - min_birth).days                                                                 
    
    # Escolhe um dia aleatório dentro do intervalo
    # random.randrange(n) devolve um número inteiro aleatório entre 0 e n-1. Como queremos incluir também o último dia (max_birth), usamos span + 1. 
    # Assim, o intervalo vai de 0 até span.
    # timedelta(days=X) representa um intervalo de tempo de X dias.
    # Quando somamos a uma date, avançamos essa data em X dias.
    # Como garantimos que min_birth é uma data válida, o resultado também será uma data válida. Quando somamos, o Python faz a aritmética de calendário automaticamente.
    return min_birth + timedelta(days=random.randrange(span + 1))                                       



def random_signup_datetime(rng: random.Random, start: datetime, end_excl: datetime) -> datetime:
    """
    Generates a "realistic" datetime for customer registration with monthly seasonality and a weekend boost.
    """
    if start >= end_excl:
        raise ValueError("`start` must be earlier than `end_excl`.")

    # 1) Define the weights by month
    month_weight = {
        1:0.85, 2:0.90, 3:1.00, 4:1.05, 5:1.10,
        6:0.95, 7:0.80, 8:0.85, 9:1.10, 10:1.20,
        11:1.30, 12:1.50
    }

    # 2) Chose a day on the interval with weights on months and weekend boost.
    days: list[tuple[datetime, float]] = []
    d = start.date()
    last = (end_excl - timedelta(seconds=1)).date()
    while d <= last:
        w = month_weight.get(d.month, 1.0)
        if d.weekday() >= 5:  # Sáb/Dom
            w *= 1.2
        days.append((datetime(d.year, d.month, d.day), w))
        d += timedelta(days=1)

    total_w = sum(w for _, w in days)
    pick_day = rng.choices([dt for dt, _ in days], weights=[w/total_w for _, w in days], k=1)[0]

    # 3) hora do dia (picos às 10–13h e 18–22h)
    hour_weights = [1.0]*24
    for h in range(10, 14): hour_weights[h] = 1.8
    for h in range(18, 23): hour_weights[h] = 2.0
    hour = rng.choices(range(24), weights=hour_weights, k=1)[0]
    minute = rng.randint(0, 59)
    second = rng.randint(0, 59)
    return pick_day.replace(hour=hour, minute=minute, second=second)

def build_created_at_schedule(
    n: int,
    start: datetime,
    end_excl: datetime,
    rng: random.Random,
) -> list[datetime]:
    """
    Gera N datetimes realistas no intervalo, ordena e força monotonia estrita (sem empates).
    """
    if n <= 0:
        return []

    pool = [random_signup_datetime(rng, start, end_excl) for _ in range(n)]
    pool.sort()  # ordem cronológica

    # garantir strictly increasing: se igual ou a recuar (improvável), empurra +1..5s
    for i in range(1, n):
        if pool[i] <= pool[i-1]:
            bump = rng.randint(1, 5)
            pool[i] = pool[i-1] + timedelta(seconds=bump)

        # cap no end_excl-1s (só para segurança)
        if pool[i] >= end_excl:
            pool[i] = end_excl - timedelta(seconds=1)

    return pool

# Função para normalizar nomes em emails
def normalize_email_names(text: str) -> str:
    
    # Remove espaços e transforma todas as letras em minúsculas
    s = text.strip().lower()
    
    s = unicodedata.normalize('NFKD', s)
    
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    
    # remove caracteres não-ascii (ex.: grego)
    s = s.encode("ascii", "ignore").decode("ascii")
    
    # mantém só [a-z0-9]
    s = re.sub(r"[^a-z0-9]+", "", s)
    
    return s

def make_unique_email(
    first_name: str,
    last_name: str,
    domain: str,
    *,
    rng: random.Random,
    used_locals: set[str],
    used_emails: set[str],
) -> str:
    fn = normalize_email_names(first_name)
    ln = normalize_email_names(last_name)
    base_local = f"{fn}.{ln}"
    domain = domain.lower().strip()

    base_email = f"{base_local}@{domain}"

    # regra: se já existir um email com o mesmo conjunto de nomes, adicionar dígito 0–9
    needs_suffix = (base_local in used_locals) or (base_email in used_emails)
    used_locals.add(base_local)

    if not needs_suffix:
        used_emails.add(base_email)
        return base_email

    # tenta com 1 dígito (0–9)
    for _ in range(25):
        digit = rng.randint(0, 9)
        email = f"{fn}.{ln}{digit}@{domain}"
        if email not in used_emails:
            used_emails.add(email)
            return email

    # fallback (se houver MUITAS colisões): garante unicidade
    while True:
        suffix = rng.randint(0, 9999)
        email = f"{fn}.{ln}{suffix}@{domain}"
        if email not in used_emails:
            used_emails.add(email)
            return email

def build_domain_schedule(n: int, dist: Mapping[str, int], rng: random.Random) -> list[str]:
    """
    Gera uma lista de domínios com contagens inteiras (o mais próximo possível da percentagem),
    e baralha para distribuir ao longo do dataset.
    """
    weights = normalize_distribution(dist)
    expected = {k: weights[k] * n for k in weights}
    counts = {k: int(expected[k]) for k in weights}
    remainder = n - sum(counts.values())

    order = sorted(weights.keys(), key=lambda k: expected[k] - counts[k], reverse=True)
    for i in range(remainder):
        counts[order[i % len(order)]] += 1

    schedule = [dom for dom, cnt in counts.items() for _ in range(cnt)]
    rng.shuffle(schedule)
    return schedule

def fetch_existing_emails(cur) -> set[str]:
    """
    Lê emails já existentes na tabela customers (para não colidir quando corres o script mais do que uma vez).
    """
    cur.execute("SELECT email FROM customers")
    return {e.lower() for (e,) in cur.fetchall() if e}

# --------------------------------------------------------------------------------------------------------------------------------------
# VALIDATIONS & SETUP
# --------------------------------------------------------------------------------------------------------------------------------------

def validate_distributions(
    country_dist: dict[str, int],
    cities_by_country: dict[str, list[str]],
    locales_by_country: dict[str, str],
) -> None:
    """
    Valida a coerência entre a distribuição de países, as cidades e os locales Faker.

    Args:
        country_dist: Distribuição de países e respetivos pesos.
        cities_by_country: Mapeamento país → lista de cidades disponíveis.
        locales_by_country: Mapeamento país → locale usado pelo Faker.

    Raises:
        ValueError: Se faltar alguma cidade, locale ou se houver cidades vazias.
    """
    
    for country in country_dist:
        if country not in cities_by_country:
            raise ValueError(f"Missing cities for country '{country}'.")
        if country not in locales_by_country:
            raise ValueError(f"Missing Faker locale for country '{country}'.")
        if not cities_by_country[country]:
            raise ValueError(f"Empty city list for country '{country}'.")
        if country_dist[country] < 0:
            raise ValueError(f"Negative weight for country '{country}'.")


def build_fakers(locales_by_country: dict[str, str], seed: int) -> dict[str, Faker]:
    """
    Cria instâncias Faker por país, com seeds fixos para reprodutibilidade.

    Args:
        locales_by_country: Mapeamento país → locale do Faker (ex: 'pt_PT').
        seed: Valor base da semente aleatória global.

    Returns:
        Dicionário {pais: Faker_instanciado}.
    """
    
    # Cria instâncias Faker específicas por país
    # Ex: fakers = {'Portugal': Faker object(pt_PT), 'Spain': Faker object(es_ES), 'France': Faker object(fr_FR), ...)
    fakers = {c: Faker(loc) for c, loc in locales_by_country.items()}
    
    # A função enumerate pega numa lista (ou outro iterável) e devolve cada elemento junto com o seu número de ordem (índice), sob a forma de pares: (índice, elemento). 
    # No caso de aplicar enumerate a fakers.items(), cada elemento é um par (chave, valor), pelo que o resultado tem a forma (índice, (chave, valor)) → um par cujo segundo elemento é um tuplo. 
    # O resultado desta função é um objeto enumerador. Esse objeto é preguiçoso (lazy): só gera os pares (índice, elemento) à medida que tu vais percorrendo. 
    # Como é iterável, para ver o conteúdo podemos convertê-lo para lista: list(enumerate(fakers.items())) = [(0, ('Portugal', <Faker pt_PT>)), (1, ('Spain', <Faker es_ES>)), (2, ('France', <Faker fr_FR>)), ...].
    for i, (country, fk) in enumerate(fakers.items()):
        
        # O método seed_instance atribui uma semente (seed) a um objeto Faker. 
        # Assim, cada instância de Faker vai gerar sempre os mesmos dados em execuções diferentes (reprodutibilidade por país). 
        # Como usamos SEED + i, cada país recebe uma semente diferente, evitando que os resultados se repitam entre países.
        fk.seed_instance(seed + i)
        
    return fakers


def fetch_country_name_to_id(cur) -> dict[str, int]:
    """
    Obtém o mapeamento nome → ID de países a partir da base de dados.

    Args:
        cur: Cursor MySQL ativo.

    Returns:
        Dicionário {nome_do_pais: country_id}.
    """
    
    cur.execute("SELECT country_id, name FROM countries")
    rows = cur.fetchall()  # [(id, name), ...]
    
    return {name: cid for cid, name in rows}
                                                
# --------------------------------------------------------------------------------------------------------------------------------------
# DATA GENERATION
# --------------------------------------------------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class CustomerRow:
    first_name: str
    last_name: str
    email: str
    birth_date: date
    city: str
    country_id: int
    created_at: datetime


def generate_customer(
    country_names: list[str],
    country_weights: Sequence[float],
    age_ranges: list[tuple[int, int]],
    age_weights: Sequence[float],
    cities_by_country: dict[str, list[str]],
    fakers: dict[str, Faker],
    name_to_id: dict[str, int],
    *,
    email_domain: str,
    email_rng: random.Random,
    used_locals: set[str],
    used_emails: set[str],
) -> tuple[str, CustomerRow]:
    """
    Gera um cliente aleatório com base nas distribuições configuradas.

    Args:
        country_names: Lista de nomes de países disponíveis.
        country_weights: Pesos normalizados para seleção de países.
        age_ranges: Faixas etárias possíveis (tuplos de idade mínima e máxima).
        age_weights: Pesos normalizados para seleção de faixas etárias.
        cities_by_country: Mapeamento país → lista de cidades.
        fakers: Instâncias Faker específicas por país.
        name_to_id: Mapeamento país → ID do país na BD.

    Returns:
        Um tuplo (country_name, CustomerRow).
    """
    
    # Seleciona país e faixa etária
    cname = random.choices(country_names, weights=country_weights, k=1)[0]
    amin, amax = random.choices(age_ranges, weights=age_weights, k=1)[0]
    
    # Gera dados pessoais e data de nascimento
    birth = random_birthdate(amin, amax)
    fk = fakers[cname]
    
    first = fk.first_name()
    last = fk.last_name()
    
    # Email
    email = make_unique_email(
        first, last, email_domain,
        rng=email_rng,
        used_locals=used_locals,
        used_emails=used_emails,
    )
    
    # Constrói o objeto CustomerRow
    row = CustomerRow(
        first_name=first,
        last_name=last,
        email=email,
        birth_date=birth,
        city=random.choice(cities_by_country[cname]),
        country_id=name_to_id[cname],
        created_at=datetime.min,  # placeholder
    )
    return cname, row


def batched(iterable: Iterable[CustomerRow], size: int) -> Iterator[list[CustomerRow]]:
    """
    Divide um iterável em listas (batches) de tamanho máximo `size`.

    Args:
        iterable: Fonte de objetos CustomerRow.
        size: Número máximo de elementos por batch.

    Yields:
        Listas de CustomerRow com até `size` elementos.
    """
    
    batch: list[CustomerRow] = []
    
    for item in iterable:
        batch.append(item)
        
        # Quando o batch atinge o tamanho máximo, envia-o e limpa
        if size and len(batch) >= size:
            yield batch
            batch = []
    
    # Garante que o último batch (incompleto) também é enviado
    if batch:
        yield batch

# --------------------------------------------------------------------------------------------------------------------------------------
# I/O NA BD
# --------------------------------------------------------------------------------------------------------------------------------------

def insert_batch(cur, rows: list[CustomerRow]) -> int:
    """
    Insere um batch de clientes na base de dados MySQL.

    Args:
        cur: Cursor MySQL ativo.
        rows: Lista de CustomerRow a inserir.

    Returns:
        Número de registos inseridos.
    """
    
    # Constrói tuplos na ordem exata das colunas do SQL_INSERT
    payload = [
        (
            r.first_name, 
            r.last_name, 
            r.email,
            r.birth_date, 
            r.city, 
            r.country_id, 
            r.created_at,
            r.created_at,  # updated_at = created_at
         )
        for r in rows
    ]
    
    # Executa inserção em batch
    cur.executemany(SQL_INSERT, payload)
    
    return len(rows)

# --------------------------------------------------------------------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------------------------------------------------------------------

def run(n_customers: int, batch_size: int, seed: int) -> None:
    start_time = time.perf_counter()
    random.seed(seed)

    # 1) valida configs e prepara Faker
    validate_distributions(COUNTRY_DISTRIBUTION, CITIES_BY_COUNTRY, FAKER_LOCALE_BY_COUNTRY)
    fakers = build_fakers(FAKER_LOCALE_BY_COUNTRY, seed)

    # 2) estruturas auxiliares
    counts_by_country = {c: 0 for c in COUNTRY_DISTRIBUTION}
    country_names = list(COUNTRY_WEIGHTS.keys())
    country_weights = list(COUNTRY_WEIGHTS.values())
    age_ranges = list(AGE_WEIGHTS.keys())
    age_weights = list(AGE_WEIGHTS.values())

    # 3) conexão e transação
    conn = get_connection()
    conn.autocommit = False
    total = batches = 0

    try:
        with conn.cursor() as cur:
            cur.execute("SET time_zone = '+00:00';")
            
            # 3.1) mapa países → id
            name_to_id = fetch_country_name_to_id(cur)
            missing = [c for c in COUNTRY_DISTRIBUTION if c not in name_to_id]
            if missing:
                raise ValueError(f"Country(ies) missing in table 'countries': {missing}")

            # 3.2) precomputar created_at sequencial (IDs baixos mais cedo)
            rng = random.Random(seed)
            created_schedule = build_created_at_schedule(
                n_customers, CUSTOMERS_START, CUSTOMERS_END_EXCL, rng
            )

            # emails já existentes (para não colidir em re-execuções)
            existing_emails = fetch_existing_emails(cur)
            used_emails: set[str] = set(existing_emails)

            # usados por "conjunto de nomes" (local-part base). Para emails existentes,
            # consideramos o local sem 1 dígito final (se houver) como "base".
            used_locals: set[str] = set()
            for e in existing_emails:
                local = e.split("@", 1)[0]
                base = local[:-1] if local and local[-1].isdigit() else local
                used_locals.add(base)

            # schedule de domínios com a distribuição pedida
            domain_rng = random.Random(seed + 999)
            domain_schedule = build_domain_schedule(n_customers, EMAIL_DOMAIN_DISTRIBUTION, domain_rng)

            # rng separado para os dígitos de colisão (reprodutível)
            email_rng = random.Random(seed + 555)
            
            # 3.3) gerador de linhas que consome o schedule
            def row_stream() -> Iterator[CustomerRow]:
                for i in range(n_customers):
                    cname, row = generate_customer(
                        country_names, country_weights, age_ranges, age_weights,
                        CITIES_BY_COUNTRY, fakers, name_to_id,
                        email_domain=domain_schedule[i],
                        email_rng=email_rng,
                        used_locals=used_locals,
                        used_emails=used_emails
                    )
                    counts_by_country[cname] += 1
                    
                    # substitui o created_at aleatório pelo sequencial
                    yield CustomerRow(
                        first_name=row.first_name,
                        last_name=row.last_name,
                        email=row.email,
                        birth_date=row.birth_date,
                        city=row.city,
                        country_id=row.country_id,
                        created_at=created_schedule[i],
        )

            # 3.3) inserir em batches
            for batch in batched(row_stream(), batch_size or n_customers):
                total += insert_batch(cur, batch)
                conn.commit()
                batches += 1

    except mysql.connector.Error as e:
        conn.rollback()
        print(f"[MySQL] {e.__class__.__name__}: {e}")
        return
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()

    # 4) métricas
    elapsed = time.perf_counter() - start_time
    print(
        f"✅ Inserted {total} customers in {batches} batch(es).\n"
        f"⏱️ Total time: {elapsed:.2f} seconds (~{(total/elapsed) if elapsed else 0:.1f} customers/second).\n"
        f"📊 Distribution by country:"
    )
    for country, count in counts_by_country.items():
        perc = (count / total) * 100 if total else 0
        target = COUNTRY_WEIGHTS[country] * 100
        print(f"   - {country}: {count} ({perc:.1f}%, target {target}%)")



# --------------------------------------------------------------------------------------------------------------------------------------
# MAIN SUPER CURTO
# --------------------------------------------------------------------------------------------------------------------------------------

def main():
    print(
        f"🔌 Connecting to database '{DB_CONFIG['database']}' "
        f"as user '{DB_CONFIG['user']}' on host '{DB_CONFIG['host']}'..."
    )
    run(N_CUSTOMERS, BATCH_SIZE, SEED)

if __name__ == "__main__":
    main()