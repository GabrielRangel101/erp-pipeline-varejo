"""
Carga da estrela: trt -> dw.

Regra unica de recorte: a carga da estrela so olha para as linhas de trt com
id_execucao = esta execucao. Isso funciona nos dois modos sem um "if":
na carga completa o tratamento acabou de reescrever trt inteiro com o id
corrente; no incremental so as linhas tocadas tem esse id.

Tudo e upsert por chave natural (ON CONFLICT), entao reprocessar a mesma
janela nao duplica. E o que permite a marca-d'agua recuar de proposito (a
janela de reprocessamento) e a carga que morreu no meio ser simplesmente
rodada de novo.

fato_estoque_dia e diferente dos outros: e DENSO (uma linha por dia x loja x
produto, mesmo sem movimento) e o saldo e acumulado, entao qualquer movimento
novo no dia D invalida D em diante. Em vez de reconstruir a historia toda, a
carga reconstroi a partir do primeiro dia afetado e puxa o saldo de abertura
do dia anterior, que ja esta no proprio fato. Carga completa e o mesmo codigo
com o primeiro dia afetado = inicio da historia.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import psycopg

from pipeline import calendario, limpeza
from pipeline.controle import Execucao

# Chave da dim_calendario a partir de uma data: 2026-11-11 -> 20261111.
SK_DATA = "(extract(year from {c})::int * 10000 " \
          "+ extract(month from {c})::int * 100 " \
          "+ extract(day from {c})::int)"


SQL_DIM_LOJA = """
INSERT INTO dw.dim_loja (sistema_origem, cod_loja, nome_loja, cidade, uf,
                         formato, dt_abertura, ativa, dt_alt_origem, id_execucao)
SELECT t.sistema_origem, t.cod_loja::smallint, t.nome_loja, t.cidade, t.uf,
       t.formato, t.dt_abertura, t.ativa, t.dt_alt_origem, %(execucao)s
  FROM trt.loja t
 WHERE t.sistema_origem = %(sistema)s AND t.id_execucao = %(execucao)s
ON CONFLICT (sistema_origem, cod_loja) DO UPDATE SET
    nome_loja = EXCLUDED.nome_loja, cidade = EXCLUDED.cidade, uf = EXCLUDED.uf,
    formato = EXCLUDED.formato, dt_abertura = EXCLUDED.dt_abertura,
    ativa = EXCLUDED.ativa, dt_alt_origem = EXCLUDED.dt_alt_origem,
    id_execucao = EXCLUDED.id_execucao, dt_carga = now()
"""

SQL_DIM_PRODUTO = """
INSERT INTO dw.dim_produto (sistema_origem, cod_produto, cod_barras, descricao,
                            departamento, secao, unid_medida, cod_fornecedor,
                            nome_fornecedor, preco_venda, custo_medio, ativo,
                            dt_alt_origem, id_execucao)
SELECT t.sistema_origem, t.cod_produto, t.cod_barras, t.descricao,
       t.departamento, t.secao, t.unid_medida, t.cod_fornecedor,
       t.nome_fornecedor, t.preco_venda, t.custo_medio, t.ativo,
       t.dt_alt_origem, %(execucao)s
  FROM trt.produto t
 WHERE t.sistema_origem = %(sistema)s AND t.id_execucao = %(execucao)s
ON CONFLICT (sistema_origem, cod_produto) DO UPDATE SET
    cod_barras = EXCLUDED.cod_barras, descricao = EXCLUDED.descricao,
    departamento = EXCLUDED.departamento, secao = EXCLUDED.secao,
    unid_medida = EXCLUDED.unid_medida, cod_fornecedor = EXCLUDED.cod_fornecedor,
    nome_fornecedor = EXCLUDED.nome_fornecedor, preco_venda = EXCLUDED.preco_venda,
    custo_medio = EXCLUDED.custo_medio, ativo = EXCLUDED.ativo,
    dt_alt_origem = EXCLUDED.dt_alt_origem,
    id_execucao = EXCLUDED.id_execucao, dt_carga = now()
"""

SQL_FATO_VENDA_ITEM = f"""
INSERT INTO dw.fato_venda_item (sistema_origem, id_item, id_venda, sk_data,
       sk_loja, sk_produto, dt_venda, qtd, vl_unit, vl_bruto, vl_desconto,
       vl_liquido, vl_custo, vl_margem, cancelado, dt_alt_origem, id_execucao)
SELECT t.sistema_origem, t.id_item, t.id_venda,
       {SK_DATA.format(c='t.dt_venda')},
       l.sk_loja, p.sk_produto,
       t.dt_venda, t.qtd, t.vl_unit, t.vl_bruto, t.vl_desconto, t.vl_liquido,
       t.vl_custo, t.vl_margem, t.cancelado, t.dt_alt_origem, %(execucao)s
  FROM trt.venda_item t
  JOIN dw.dim_loja l    ON l.sistema_origem = t.sistema_origem
                       AND l.cod_loja = t.cod_loja
  JOIN dw.dim_produto p ON p.sistema_origem = t.sistema_origem
                       AND p.cod_produto = t.cod_produto
 WHERE t.sistema_origem = %(sistema)s AND t.id_execucao = %(execucao)s
ON CONFLICT (sistema_origem, id_item) DO UPDATE SET
    id_venda = EXCLUDED.id_venda, sk_data = EXCLUDED.sk_data,
    sk_loja = EXCLUDED.sk_loja, sk_produto = EXCLUDED.sk_produto,
    dt_venda = EXCLUDED.dt_venda, qtd = EXCLUDED.qtd, vl_unit = EXCLUDED.vl_unit,
    vl_bruto = EXCLUDED.vl_bruto, vl_desconto = EXCLUDED.vl_desconto,
    vl_liquido = EXCLUDED.vl_liquido, vl_custo = EXCLUDED.vl_custo,
    vl_margem = EXCLUDED.vl_margem, cancelado = EXCLUDED.cancelado,
    dt_alt_origem = EXCLUDED.dt_alt_origem,
    id_execucao = EXCLUDED.id_execucao, dt_carga = now()
"""

# Retrato denso de estoque. Le-se de baixo para cima:
#   mov      movimento do periodo afetado, somado por dia/loja/produto, com o
#            sinal ja aplicado (entrada positiva, saida negativa).
#   pares    todo par loja x produto que ja teve movimento, e desde quando.
#   grade    um dia para cada par, do primeiro dia afetado ate o fim da
#            historia. E o que torna o fato DENSO: dia sem movimento tambem
#            vira linha, senao nao da para medir ruptura.
#   abertura saldo do dia anterior ao recorte, lido do proprio fato. E o que
#            permite reconstruir so o pedaco afetado em vez da historia toda.
SQL_FATO_ESTOQUE_DIA = f"""
WITH mov AS (
    SELECT sistema_origem, cod_loja, cod_produto, dia,
           sum(sinal) FILTER (WHERE tp_mov = 'CMP')              AS entrada_compra,
           -sum(sinal) FILTER (WHERE tp_mov = 'VND')             AS saida_venda,
           sum(sinal) FILTER (WHERE tp_mov NOT IN ('CMP','VND')) AS outros,
           sum(sinal)                                            AS delta
      FROM (SELECT sistema_origem, cod_loja, cod_produto, tp_mov,
                   dt_mov::date AS dia,
                   CASE WHEN sentido = 'E' THEN qtd ELSE -qtd END AS sinal
              FROM trt.estoque_mov
             WHERE sistema_origem = %(sistema)s
               AND dt_mov::date >= %(desde)s::date) m
     GROUP BY 1, 2, 3, 4
),
pares AS (
    SELECT sistema_origem, cod_loja, cod_produto,
           min(dt_mov::date) AS dia_inicial
      FROM trt.estoque_mov
     WHERE sistema_origem = %(sistema)s
     GROUP BY 1, 2, 3
),
limite AS (
    SELECT max(dt_mov::date) AS dia_final
      FROM trt.estoque_mov WHERE sistema_origem = %(sistema)s
),
grade AS (
    SELECT p.sistema_origem, p.cod_loja, p.cod_produto, d::date AS dia
      FROM pares p
      CROSS JOIN limite
      CROSS JOIN LATERAL generate_series(
              greatest(p.dia_inicial, %(desde)s::date),
              limite.dia_final, interval '1 day') AS d
),
abertura AS (
    SELECT l.cod_loja, p.cod_produto, f.qtd_saldo_final AS saldo_inicial
      FROM dw.fato_estoque_dia f
      JOIN dw.dim_loja l    ON l.sk_loja = f.sk_loja
      JOIN dw.dim_produto p ON p.sk_produto = f.sk_produto
     WHERE f.sistema_origem = %(sistema)s
       AND f.sk_data = {SK_DATA.format(c="(%(desde)s::date - 1)")}
),
calculado AS (
    SELECT g.sistema_origem, g.cod_loja, g.cod_produto, g.dia,
           coalesce(m.entrada_compra, 0) AS qtd_entrada_compra,
           coalesce(m.saida_venda, 0)    AS qtd_saida_venda,
           coalesce(m.outros, 0)         AS qtd_outros,
           coalesce(a.saldo_inicial, 0)
             + sum(coalesce(m.delta, 0)) OVER (
                   PARTITION BY g.sistema_origem, g.cod_loja, g.cod_produto
                   ORDER BY g.dia) AS qtd_saldo_final
      FROM grade g
      LEFT JOIN mov m ON m.sistema_origem = g.sistema_origem
                     AND m.cod_loja = g.cod_loja
                     AND m.cod_produto = g.cod_produto
                     AND m.dia = g.dia
      LEFT JOIN abertura a ON a.cod_loja = g.cod_loja
                          AND a.cod_produto = g.cod_produto
)
INSERT INTO dw.fato_estoque_dia (sistema_origem, sk_data, sk_loja, sk_produto,
       qtd_entrada_compra, qtd_saida_venda, qtd_outros, qtd_saldo_final,
       em_ruptura, id_execucao)
SELECT c.sistema_origem, {SK_DATA.format(c='c.dia')}, l.sk_loja, p.sk_produto,
       c.qtd_entrada_compra, c.qtd_saida_venda, c.qtd_outros, c.qtd_saldo_final,
       (c.qtd_saldo_final <= 0), %(execucao)s
  FROM calculado c
  JOIN dw.dim_loja l    ON l.sistema_origem = c.sistema_origem
                       AND l.cod_loja = c.cod_loja
  JOIN dw.dim_produto p ON p.sistema_origem = c.sistema_origem
                       AND p.cod_produto = c.cod_produto
ON CONFLICT (sistema_origem, sk_data, sk_loja, sk_produto) DO UPDATE SET
    qtd_entrada_compra = EXCLUDED.qtd_entrada_compra,
    qtd_saida_venda = EXCLUDED.qtd_saida_venda,
    qtd_outros = EXCLUDED.qtd_outros,
    qtd_saldo_final = EXCLUDED.qtd_saldo_final,
    em_ruptura = EXCLUDED.em_ruptura,
    id_execucao = EXCLUDED.id_execucao, dt_carga = now()
"""


@dataclass
class ResultadoEstrela:
    dim_loja: int = 0
    dim_produto: int = 0
    dim_calendario: int = 0
    fato_venda_item: int = 0
    fato_estoque_dia: int = 0
    estoque_desde: date | None = None


def intervalo_tratado(conn: psycopg.Connection,
                      sistema: str) -> tuple[date, date] | None:
    """Primeiro e ultimo dia com dado tratado (vendas e movimentos juntos)."""
    inicio, fim = conn.execute(
        "SELECT min(dia), max(dia) FROM ("
        "  SELECT min(dt_venda::date) AS dia FROM trt.venda_item "
        "   WHERE sistema_origem = %(s)s"
        "  UNION ALL SELECT max(dt_venda::date) FROM trt.venda_item "
        "   WHERE sistema_origem = %(s)s"
        "  UNION ALL SELECT min(dt_mov::date) FROM trt.estoque_mov "
        "   WHERE sistema_origem = %(s)s"
        "  UNION ALL SELECT max(dt_mov::date) FROM trt.estoque_mov "
        "   WHERE sistema_origem = %(s)s) d",
        {"s": sistema}).fetchone()
    if inicio is None or fim is None:
        return None
    return inicio, fim


def _primeiro_dia_afetado(conn: psycopg.Connection, sistema: str,
                          id_execucao: int, completa: bool) -> date | None:
    """De que dia em diante o fato denso precisa ser reconstruido."""
    if completa:
        linha = conn.execute(
            "SELECT min(dt_mov::date) FROM trt.estoque_mov "
            "WHERE sistema_origem = %s", (sistema,)).fetchone()
    else:
        linha = conn.execute(
            "SELECT min(dt_mov::date) FROM trt.estoque_mov "
            "WHERE sistema_origem = %s AND id_execucao = %s",
            (sistema, id_execucao)).fetchone()
    return linha[0]


def carregar(conn: psycopg.Connection, execucao: Execucao,
             completa: bool) -> ResultadoEstrela:
    parametros = {"sistema": execucao.sistema_origem,
                  "execucao": execucao.id_execucao}
    resultado = ResultadoEstrela()
    cur = conn.cursor()

    # 1. Calendario antes dos fatos: dw.fato_* tem FK para dim_calendario.
    intervalo = intervalo_tratado(conn, execucao.sistema_origem)
    if intervalo is not None:
        with execucao.cronometrar("estrela", "dim_calendario") as marca:
            resultado.dim_calendario = calendario.garantir(conn, *intervalo)
            marca["escritas"] = resultado.dim_calendario

    # 2. Dimensoes antes dos fatos: o fato so entra com sk resolvida, nunca
    #    com linha "desconhecido". Quem nao tem dimensao valida ja ficou na
    #    quarentena, la na camada tratada.
    for etapa, sql_dim in (("dim_loja", SQL_DIM_LOJA),
                           ("dim_produto", SQL_DIM_PRODUTO)):
        with execucao.cronometrar("estrela", etapa) as marca:
            cur.execute(sql_dim, parametros)
            marca["escritas"] = cur.rowcount
            setattr(resultado, etapa, cur.rowcount)
            # ANALYZE logo apos recarregar a dimensao, ANTES de o fato juntar
            # com ela: a carga completa TRUNCA a dimensao, e ate o autoanalyze
            # passar o planner acha que ela esta vazia e escolhe um plano ruim
            # para o join de 1 milhao de linhas do fato. Medido: 78,99 s -> 21,91 s.
            #
            # So na carga completa. No incremental a dimensao nao foi truncada,
            # a estatistica continua boa e o ANALYZE seria so custo fixo (medido:
            # +2,7 s numa carga incremental de 14 mil itens, que leva 2,1 s).
            if completa:
                cur.execute(f"ANALYZE dw.{etapa}")
        conn.commit()

    with execucao.cronometrar("estrela", "fato_venda_item") as marca:
        cur.execute(SQL_FATO_VENDA_ITEM, parametros)
        resultado.fato_venda_item = cur.rowcount
        marca["escritas"] = cur.rowcount
    conn.commit()

    # 3. Fato denso de estoque.
    if completa:
        cur.execute("DELETE FROM dw.fato_estoque_dia WHERE sistema_origem = %s",
                    (execucao.sistema_origem,))
    desde = _primeiro_dia_afetado(conn, execucao.sistema_origem,
                                  execucao.id_execucao, completa)
    resultado.estoque_desde = desde
    if desde is not None:
        with execucao.cronometrar("estrela", "fato_estoque_dia") as marca:
            cur.execute(SQL_FATO_ESTOQUE_DIA, {**parametros, "desde": desde})
            resultado.fato_estoque_dia = cur.rowcount
            marca["escritas"] = cur.rowcount
    conn.commit()
    return resultado


def limpar_estrela(conn: psycopg.Connection, sistema_origem: str) -> str:
    """Carga completa: zera a estrela deste sistema.

    dim_calendario fica de fora: ela nao tem sistema_origem e e recalculada
    por upsert. Ver limpeza.py para o porque de nao ser um DELETE direto.
    """
    return limpeza.limpar(conn, "dw", ["fato_venda_item", "fato_estoque_dia",
                                       "dim_produto", "dim_loja"],
                          sistema_origem)
