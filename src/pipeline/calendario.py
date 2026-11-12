"""
dim_calendario: uma linha por dia, com feriado nacional.

Por que em Python e nao em generate_series: feriado movel depende da Pascoa, e
a Pascoa e um calculo (algoritmo de Gauss/Meeus) que fica ilegivel em SQL.

PRECISAO DO QUE E FERIADO
  eh_feriado_nacional = true  SO para feriado nacional de verdade (Lei 662/1949,
  Lei 10.607/2002, Lei 9.093/1995 para a Sexta-feira Santa e Lei 14.759/2023
  para o 20 de novembro, nacional a partir de 2024).

  Carnaval, Quarta-feira de Cinzas e Corpus Christi NAO sao feriado nacional,
  sao ponto facultativo. Mas movem venda de supermercado tanto quanto feriado,
  entao entram com nome_feriado preenchido e eh_feriado_nacional = false. Quem
  analisa demanda filtra por nome_feriado IS NOT NULL; quem precisa do conceito
  legal filtra por eh_feriado_nacional.

Nomes de mes e dia vao sem acento, igual ao resto do projeto: evita surpresa de
codificacao em log de CI no Windows.
"""

from __future__ import annotations

from datetime import date, timedelta

import psycopg

MESES = ["janeiro", "fevereiro", "marco", "abril", "maio", "junho", "julho",
         "agosto", "setembro", "outubro", "novembro", "dezembro"]

DIAS_SEMANA = ["segunda-feira", "terca-feira", "quarta-feira", "quinta-feira",
               "sexta-feira", "sabado", "domingo"]

# (mes, dia) -> (nome, ano a partir do qual vale)
FERIADOS_FIXOS: dict[tuple[int, int], tuple[str, int]] = {
    (1, 1):   ("Confraternizacao Universal", 0),
    (4, 21):  ("Tiradentes", 0),
    (5, 1):   ("Dia do Trabalho", 0),
    (9, 7):   ("Independencia do Brasil", 0),
    (10, 12): ("Nossa Senhora Aparecida", 0),
    (11, 2):  ("Finados", 0),
    (11, 15): ("Proclamacao da Republica", 0),
    (12, 25): ("Natal", 0),
    # Nacional so a partir de 2024 (Lei 14.759/2023).
    (11, 20): ("Zumbi e Consciencia Negra", 2024),
}


def pascoa(ano: int) -> date:
    """Domingo de Pascoa pelo algoritmo anonimo gregoriano."""
    a = ano % 19
    b, c = divmod(ano, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    ll = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * ll) // 451
    mes, dia = divmod(h + ll - 7 * m + 114, 31)
    return date(ano, mes, dia + 1)


def feriados_do_ano(ano: int) -> dict[date, tuple[str, bool]]:
    """data -> (nome, eh_feriado_nacional)."""
    saida: dict[date, tuple[str, bool]] = {}
    for (mes, dia), (nome, desde) in FERIADOS_FIXOS.items():
        if ano >= desde:
            saida[date(ano, mes, dia)] = (nome, True)

    domingo = pascoa(ano)
    saida[domingo - timedelta(days=2)] = ("Sexta-feira Santa", True)
    # Ponto facultativo, nao feriado nacional - mas pesa no varejo.
    saida[domingo - timedelta(days=48)] = ("Carnaval (facultativo)", False)
    saida[domingo - timedelta(days=47)] = ("Carnaval (facultativo)", False)
    saida[domingo - timedelta(days=46)] = ("Quarta-feira de Cinzas (facultativo)", False)
    saida[domingo + timedelta(days=60)] = ("Corpus Christi (facultativo)", False)
    return saida


def linhas(inicio: date, fim: date) -> list[tuple]:
    """Linhas de dw.dim_calendario entre inicio e fim, inclusive."""
    if fim < inicio:
        raise ValueError(f"intervalo invalido: {inicio} > {fim}")

    feriados: dict[date, tuple[str, bool]] = {}
    for ano in range(inicio.year, fim.year + 1):
        feriados.update(feriados_do_ano(ano))

    saida = []
    dia = inicio
    while dia <= fim:
        nome_feriado, nacional = feriados.get(dia, (None, False))
        dia_semana = dia.weekday()            # 0 = segunda
        saida.append((
            dia.year * 10000 + dia.month * 100 + dia.day,   # sk_data
            dia,
            dia.year,
            (dia.month - 1) // 3 + 1,
            dia.month,
            MESES[dia.month - 1],
            dia.isocalendar().week,
            dia.day,
            dia_semana + 1,                   # 1 = segunda ... 7 = domingo
            DIAS_SEMANA[dia_semana],
            dia_semana >= 5,
            nacional,
            nome_feriado,
        ))
        dia += timedelta(days=1)
    return saida


COLUNAS = ("sk_data", "data", "ano", "trimestre", "mes", "nome_mes",
           "semana_iso", "dia", "dia_semana", "nome_dia_semana",
           "eh_fim_de_semana", "eh_feriado_nacional", "nome_feriado")


def garantir(conn: psycopg.Connection, inicio: date, fim: date) -> int:
    """Preenche dim_calendario no intervalo pedido, com uma folga de 1 ano.

    A folga evita que a primeira venda de janeiro do ano seguinte esbarre na FK
    da dimensao. Repetivel: upsert por sk_data.

    NAO COMITA: roda dentro da transacao de publicacao da carga (ver carga.py).
    Quem chama decide quando publicar.
    """
    inicio = date(inicio.year, 1, 1)
    fim = date(fim.year + 1, 12, 31)
    dados = linhas(inicio, fim)

    colunas = ", ".join(COLUNAS)
    atribuicoes = ", ".join(f"{c} = EXCLUDED.{c}" for c in COLUNAS if c != "sk_data")
    marcadores = ", ".join(["%s"] * len(COLUNAS))
    with conn.cursor() as cur:
        cur.executemany(
            f"INSERT INTO dw.dim_calendario ({colunas}) VALUES ({marcadores}) "
            f"ON CONFLICT (sk_data) DO UPDATE SET {atribuicoes}",
            dados)
    return len(dados)
