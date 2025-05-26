import psycopg2
from psycopg2.extras import RealDictCursor
from config import POSTGRES

# Conectar ao banco
def conectar():
    conn = psycopg2.connect(**POSTGRES)
    print("🔌 Conectado ao banco:", conn.get_dsn_parameters()["dbname"])
    return conn

# Criar tabelas
def criar_tabelas():
    sql = """
    CREATE TABLE IF NOT EXISTS produto (
      id SERIAL PRIMARY KEY,
      nome VARCHAR(100) NOT NULL,
      categoria VARCHAR(50),
      marca VARCHAR(50),
      preco NUMERIC(10,2),
      estoque INT
    );

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

    CREATE TABLE IF NOT EXISTS dim_local (
      id SERIAL PRIMARY KEY,
      cidade VARCHAR(100),
      estado VARCHAR(100),
      pais VARCHAR(100),
      UNIQUE (cidade, estado, pais)
    );

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

# Inserir produto
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

# Registrar venda
def registrar_venda(pid, qtd, total, data_hora, cidade, estado, pais):
    conn = conectar()
    with conn:
        with conn.cursor() as cur:
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

            cur.execute(
                """
                INSERT INTO dim_local (cidade, estado, pais)
                VALUES (%s, %s, %s)
                ON CONFLICT (cidade, estado, pais) DO NOTHING
                """,
                (cidade, estado, pais),
            )

            cur.execute(
                """
                SELECT id FROM dim_local
                WHERE cidade = %s AND estado = %s AND pais = %s
                """,
                (cidade, estado, pais),
            )
            id_local = cur.fetchone()[0]

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

# Consulta de vendas por hora
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

# 🔍 Dados para previsão
def get_dados_para_previsao():
    conn = conectar()
    sql = """
      SELECT
        EXTRACT(YEAR FROM data_hora) AS ano,
        EXTRACT(MONTH FROM data_hora) AS mes,
        EXTRACT(DAY FROM data_hora) AS dia,
        EXTRACT(HOUR FROM data_hora) AS hora,
        SUM(valor_total) AS total_vendas
      FROM fato_venda
      GROUP BY ano, mes, dia, hora
      ORDER BY ano, mes, dia, hora;
    """
    with conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    conn.close()
    return rows

# 📈 Previsão de vendas
import pandas as pd
from sklearn.linear_model import LinearRegression
import numpy as np
import matplotlib.pyplot as plt

def prever_vendas():
    """
    Usa regressão linear simples para prever vendas com base no tempo (hora sequencial).
    """
    dados = get_dados_para_previsao()
    if not dados:
        print("⚠️ Nenhum dado encontrado para previsão.")
        return

    df = pd.DataFrame(dados)
    df["tempo"] = np.arange(len(df))  # tempo como sequência
    X = df[["tempo"]]
    y = df["total_vendas"]

    modelo = LinearRegression()
    modelo.fit(X, y)

    futuro_tempo = np.arange(len(df), len(df) + 5).reshape(-1, 1)
    previsoes = modelo.predict(futuro_tempo)

    print("📊 Previsões para os próximos 5 períodos:")
    for i, p in enumerate(previsoes):
        print(f"Período +{i+1}: R$ {p:.2f}")

    # Exibir gráfico
    plt.plot(df["tempo"], y, label="Histórico")
    plt.plot(futuro_tempo, previsoes, label="Previsão", linestyle="--")
    plt.xlabel("Tempo (período)")
    plt.ylabel("Total de Vendas")
    plt.legend()
    plt.title("Previsão de Vendas (Regressão Linear)")
    plt.grid(True)
    plt.show()

# Execução direta
if __name__ == "__main__":
    print("▶️ Rodando script com previsão de vendas...")
    criar_tabelas()
    prever_vendas()
