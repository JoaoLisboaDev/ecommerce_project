import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASS", ""),
    "database": os.getenv("DB_NAME", "ecommerce_db"),
    "charset": "utf8mb4",
}

# Tabelas a limpar (ordem do mais dependente para o menos, por clareza)
TABLES = [
    "product_returns",
    "payments",
    "order_items",
    "orders",
]

def reset_tables(conn, tables: list[str]) -> None:
    """
    Faz TRUNCATE às tabelas indicadas, com FOREIGN_KEY_CHECKS desativado,
    de modo a não ter problemas de FKs e a repor o AUTO_INCREMENT.
    """
    with conn.cursor() as cur:
        print(">> Desativar FOREIGN_KEY_CHECKS")
        cur.execute("SET FOREIGN_KEY_CHECKS = 0;")

        for t in tables:
            print(f">> TRUNCATE {t}")
            cur.execute(f"TRUNCATE TABLE `{t}`;")

        print(">> Reativar FOREIGN_KEY_CHECKS")
        cur.execute("SET FOREIGN_KEY_CHECKS = 1;")

    # TRUNCATE faz commit implícito, mas garantimos estado consistente
    conn.commit()

def main():
    print(f"🔌 A ligar à BD '{DB_CONFIG['database']}' em {DB_CONFIG['host']}:{DB_CONFIG['port']} como '{DB_CONFIG['user']}'...")
    conn = mysql.connector.connect(use_pure=True, **DB_CONFIG)
    try:
        # Opcional: garantir timezone determinística
        with conn.cursor() as cur:
            cur.execute("SET time_zone = '+00:00';")

        reset_tables(conn, TABLES)
        print("✅ Tabelas limpas e AUTO_INCREMENT reposto.")
    except mysql.connector.Error as e:
        print(f"[MySQL] {e.__class__.__name__}: {e}")
        raise
    finally:
        conn.close()
        print("🔒 Ligação fechada.")

if __name__ == "__main__":
    main()