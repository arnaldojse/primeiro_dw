import psycopg2
from psycopg2.extras import RealDictCursor
from config import POSTGRES

def conectar():
    conn = psycopg2.connect(**POSTGRES)
    print("🔌 Conectado ao banco:", conn.get_dsn_parameters()["dbname"])
    return conn

def criar_tabelas():
    sql = """
    -- Tabela de produtos (objeto-relacional)
    CREATE TABLE IF NOT EXISTS produto (
      id SERIAL PRIMARY KEY,
      nome VARCHAR(100) NOT NULL,
      categoria VARCHAR(50),
      marca VARCHAR(50),
      preco NUMERIC(10,2),
      estoque INT
    );

    -- Tabela de vendas operacionais
    CREATE TABLE IF NOT EXISTS venda (
      id SERIAL PRIMARY KEY,
      produto_id INT REFERENCES produto(id),
      quantidade INT NOT NULL,
      valor_total NUMERIC(12,2),
      data_venda DATE NOT NULL
    );

    -- Dimensão tempo
    CREATE TABLE IF NOT EXISTS dim_tempo (
      data DATE PRIMARY KEY,
      ano INT, mes INT, dia INT, trimestre INT
    );

    -- Dimensão local
    CREATE TABLE IF NOT EXISTS dim_local (
      id SERIAL PRIMARY KEY,
      cidade VARCHAR(100),
      estado VARCHAR(100),
      pais VARCHAR(100),
      UNIQUE (cidade, estado, pais)
    );

    -- Fato vendas com localização
    CREATE TABLE IF NOT EXISTS fato_venda (
      id SERIAL PRIMARY KEY,
      data DATE REFERENCES dim_tempo(data),
      produto_id INT,
      quantidade INT,
      valor_total NUMERIC(12,2),
      local_id INT REFERENCES dim_local(id)
    );
    """
    conn = conectar()
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql)
    conn.close()


def inserir_produto(prod):
    conn = conectar()
    sql = """
      INSERT INTO produto (nome,categoria,marca,preco,estoque)
      VALUES (%s,%s,%s,%s,%s) RETURNING id;
    """
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql, (prod.nome, prod.categoria, prod.marca, prod.preco, prod.estoque))
            pid = cur.fetchone()[0]
    conn.close()
    return pid

def registrar_venda(pid, qtd, total, data, cidade, estado, pais):
    conn = conectar()
    with conn:
        with conn.cursor() as cur:
            # 1. Registrar venda operacional
            cur.execute(
                """
                INSERT INTO venda
                  (produto_id, quantidade, valor_total, data_venda)
                VALUES (%s, %s, %s, %s)
                """,
                (pid, qtd, total, data),
            )

            # 2. Atualizar dimensão tempo
            cur.execute(
                """
                INSERT INTO dim_tempo (data, ano, mes, dia, trimestre)
                VALUES (
                  %s,
                  EXTRACT(YEAR FROM %s::date),
                  EXTRACT(MONTH FROM %s::date),
                  EXTRACT(DAY FROM %s::date),
                  EXTRACT(QUARTER FROM %s::date)
                )
                ON CONFLICT (data) DO NOTHING
                """,
                (data, data, data, data, data),
            )

            # 3. Atualizar dimensão local
            cur.execute(
                """
                INSERT INTO dim_local (cidade, estado, pais)
                VALUES (%s, %s, %s)
                ON CONFLICT (cidade, estado, pais) DO NOTHING
                """,
                (cidade, estado, pais),
            )

            # 4. Obter id da localização
            cur.execute(
                """
                SELECT id FROM dim_local
                WHERE cidade = %s AND estado = %s AND pais = %s
                """,
                (cidade, estado, pais),
            )
            id_local = cur.fetchone()[0]

            # 5. Inserir fato de venda
            cur.execute(
                """
                INSERT INTO fato_venda (data, produto_id, quantidade, valor_total, local_id)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (data, pid, qtd, total, id_local),
            )
    conn.close()

def fetch_vendas_por_trimestre():
    conn = conectar()
    sql = """
      SELECT ano, trimestre, SUM(valor_total) AS total
      FROM fato_venda
      JOIN dim_tempo USING(data)
      GROUP BY ano, trimestre
      ORDER BY ano, trimestre;
    """
    with conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    conn.close()
    return rows

if __name__ == "__main__":
    print("▶️ Rodando script...")
    criar_tabelas()
    print("✅ Tabelas criadas com sucesso.")
