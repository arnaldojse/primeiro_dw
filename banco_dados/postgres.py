import psycopg2
from psycopg2.extras import RealDictCursor
from config import POSTGRES

def conectar():
    conn = psycopg2.connect(**POSTGRES)
    print("🔌 Conectado ao banco:", conn.get_dsn_parameters()["dbname"])
    return conn

def criar_tabelas():
    sql = """
    -- Tabela de produtos
    CREATE TABLE IF NOT EXISTS produto (
      id SERIAL PRIMARY KEY,
      nome VARCHAR(100) NOT NULL,
      categoria VARCHAR(50),
      marca VARCHAR(50),
      preco NUMERIC(10,2),
      estoque INT
    );

    -- Dimensão tempo com timestamp incluindo hora, minuto e segundo
    CREATE TABLE IF NOT EXISTS dim_tempo (
      data_hora TIMESTAMP PRIMARY KEY,
      ano INT,
      mes INT,
      dia INT,
      hora INT,
      minuto INT,
      segundo INT,
      trimestre INT
    );

    -- Dimensão local
    CREATE TABLE IF NOT EXISTS dim_local (
      id SERIAL PRIMARY KEY,
      cidade VARCHAR(100),
      estado VARCHAR(100),
      pais VARCHAR(100),
      UNIQUE (cidade, estado, pais)
    );

    -- Fato vendas com campos temporais de criação e atualização da venda
    CREATE TABLE IF NOT EXISTS fato_venda (
      id SERIAL PRIMARY KEY,
      data_hora TIMESTAMP REFERENCES dim_tempo(data_hora),
      produto_id INT,
      quantidade INT,
      valor_total NUMERIC(12,2),
      local_id INT REFERENCES dim_local(id),
      data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
      INSERT INTO produto (nome, categoria, marca, preco, estoque)
      VALUES (%s, %s, %s, %s, %s) RETURNING id;
    """
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql, (prod.nome, prod.categoria, prod.marca, prod.preco, prod.estoque))
            pid = cur.fetchone()[0]
    conn.close()
    return pid

def registrar_venda(pid, qtd, total, data_hora, cidade, estado, pais):
    conn = conectar()
    with conn:
        with conn.cursor() as cur:
            # Atualizar dimensão tempo incluindo hora, minuto, segundo
            cur.execute(
                """
                INSERT INTO dim_tempo (
                  data_hora, ano, mes, dia, hora, minuto, segundo, trimestre
                )
                VALUES (
                  %s,
                  EXTRACT(YEAR FROM %s::timestamp),
                  EXTRACT(MONTH FROM %s::timestamp),
                  EXTRACT(DAY FROM %s::timestamp),
                  EXTRACT(HOUR FROM %s::timestamp),
                  EXTRACT(MINUTE FROM %s::timestamp),
                  EXTRACT(SECOND FROM %s::timestamp),
                  EXTRACT(QUARTER FROM %s::timestamp)
                )
                ON CONFLICT (data_hora) DO NOTHING
                """,
                (data_hora, data_hora, data_hora, data_hora, data_hora, data_hora, data_hora, data_hora)
            )

            # Atualizar dimensão local
            cur.execute(
                """
                INSERT INTO dim_local (cidade, estado, pais)
                VALUES (%s, %s, %s)
                ON CONFLICT (cidade, estado, pais) DO NOTHING
                """,
                (cidade, estado, pais),
            )

            # Obter id da localização
            cur.execute(
                """
                SELECT id FROM dim_local
                WHERE cidade = %s AND estado = %s AND pais = %s
                """,
                (cidade, estado, pais),
            )
            id_local = cur.fetchone()[0]

            # Inserir fato de venda com timestamp temporal e datas de criação/atualização
            cur.execute(
                """
                INSERT INTO fato_venda (
                  data_hora, produto_id, quantidade, valor_total, local_id,
                  data_criacao, data_atualizacao
                )
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                (data_hora, pid, qtd, total, id_local),
            )
    conn.close()

def fetch_vendas_por_hora():
    conn = conectar()
    sql = """
      SELECT
        ano, mes, dia, hora,
        SUM(valor_total) AS total_vendas
      FROM fato_venda
      JOIN dim_tempo USING(data_hora)
      GROUP BY ano, mes, dia, hora
      ORDER BY ano, mes, dia, hora;
    """
    with conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    conn.close()
    return rows

if __name__ == "__main__":
    print("▶️ Rodando script temporal...")
    criar_tabelas()
    print("✅ Tabelas criadas com sucesso.")

