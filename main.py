from datetime import date
from banco_dados import postgres

postgres.registrar_venda(
    pid=1,
    qtd=2,
    total=199.90,
    data=date(2025, 5, 24),
    cidade="São Paulo",
    estado="SP",
    pais="Brasil"
)
print("✔ Venda registrada e script finalizado.")