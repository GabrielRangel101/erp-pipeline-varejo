"""
Conferencia: a carga so termina OK se a conta fechar.

    origem == destino + quarentena

Por dia e loja, em CONTAGEM e em VALOR. Se nao bate, a carga falha com codigo
de saida != 0 e o motivo fica em ctl.conferencia, dia a dia.

POR QUE ISSO E O PONTO DO PROJETO INTEIRO
Um pipeline que entrega numero errado em silencio e pior do que um pipeline
que nao roda: o relatorio sai bonito e a decisao sai torta. Todo o resto do
desenho existe para tornar esta conta possivel - staging persistente,
quarentena por referencia em vez de copia, fato que mantem cancelado. A
conferencia e so onde isso vira um "sim" ou um "nao".

O QUE E COMPARADO

  venda_item    origem (erp.mov_venda_item)
                vs dw.fato_venda_item + quarentena de venda_item
                valor = vl_total da origem / vl_liquido da estrela

  estoque_mov   origem (erp.mov_estoque)
                vs trt.estoque_mov + quarentena de estoque_mov
                valor = qtd

  Em estoque o destino e a camada TRATADA, nao a estrela. fato_estoque_dia e
  um retrato DENSO e agregado (uma linha por dia x loja x produto, mesmo sem
  movimento), entao nao existe correspondencia 1 para 1 com o movimento da
  origem. Conferir contra a tratada prova que nada se perdeu entre a origem e
  o ultimo ponto onde ainda ha uma linha por movimento. O que acontece da
  tratada para o fato denso NAO esta coberto aqui - ver "limites conhecidos".

AGRUPAMENTO, INCLUSIVE O QUE NAO TEM DIA NEM LOJA
Item orfao (cabecalho inexistente) nao tem dia nem loja. Os dois lados usam
LEFT JOIN, entao ele cai no grupo (NULL, NULL) dos DOIS lados e continua
sendo contado. Sem isso, "nao sumiu nada" seria uma frase, nao uma conta.

JANELA
Por padrao confere a historia inteira. E proposital: a linha que o ERP gravou
com dt_alt muito antigo (fora da janela de reprocessamento) NAO esta nos dias
que esta carga tocou - conferir so o que a carga mexeu deixaria passar
justamente o caso que a conferencia existe para pegar. --conferir-desde
limita o periodo em troca de perder essa garantia.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal

import psycopg

from pipeline.controle import Execucao

# Os valores atravessam o pipeline como numeric e nao deveriam variar nada.
# A tolerancia existe so para nao transformar ruido de arredondamento em
# falha de carga. Contagem NAO tem tolerancia: tem que bater exato.
TOLERANCIA_VALOR = Decimal("0.01")

# Quantos dias divergentes aparecem na mensagem de erro antes do "e mais N".
MAX_DIVERGENCIAS_NA_MENSAGEM = 10


class ConferenciaFalhou(RuntimeError):
    """A conta nao fechou. Interrompe a carga antes de avancar a marca-d'agua."""


@dataclass(frozen=True)
class Verificacao:
    nome: str
    descricao_valor: str      # o que "valor" significa nesta verificacao
    sql_origem: str           # roda em erp_origem, como erp_ro
    sql_destino: str          # roda em dw_varejo, como pipeline
    sql_orfas_na_quarentena: str   # guarda: quarentena sem linha no staging


VERIFICACOES: tuple[Verificacao, ...] = (

    Verificacao(
        nome="venda_item",
        descricao_valor="vl_total",
        sql_origem="""
            SELECT v.dt_venda::date AS dia, v.cod_loja::int AS cod_loja,
                   count(*)::bigint AS qtd,
                   coalesce(sum(i.vl_total), 0) AS valor
              FROM erp.mov_venda_item i
              LEFT JOIN erp.mov_venda v ON v.id_venda = i.id_venda
             WHERE %(desde)s::date IS NULL
                OR v.dt_venda IS NULL
                OR v.dt_venda >= %(desde)s::date
             GROUP BY 1, 2
        """,
        sql_destino="""
            SELECT dia, cod_loja,
                   sum(qtd_destino)::bigint     AS qtd_destino,
                   sum(vl_destino)              AS vl_destino,
                   sum(qtd_quarentena)::bigint  AS qtd_quarentena,
                   sum(vl_quarentena)           AS vl_quarentena
              FROM (
                SELECT c.data AS dia, l.cod_loja::int AS cod_loja,
                       count(*) AS qtd_destino,
                       coalesce(sum(f.vl_liquido), 0) AS vl_destino,
                       0 AS qtd_quarentena, 0::numeric AS vl_quarentena
                  FROM dw.fato_venda_item f
                  JOIN dw.dim_calendario c ON c.sk_data = f.sk_data
                  JOIN dw.dim_loja l       ON l.sk_loja = f.sk_loja
                 WHERE f.sistema_origem = %(sistema)s
                   AND (%(desde)s::date IS NULL OR c.data >= %(desde)s::date)
                 GROUP BY 1, 2
                UNION ALL
                SELECT sv.dt_venda::date, sv.cod_loja::int,
                       0, 0::numeric,
                       count(*), coalesce(sum(si.vl_total), 0)
                  FROM trt.quarentena q
                  JOIN stg.venda_item si ON si.sistema_origem = q.sistema_origem
                                        AND si.id_item::text = q.chave
                  LEFT JOIN stg.venda sv ON sv.sistema_origem = si.sistema_origem
                                        AND sv.id_venda = si.id_venda
                 WHERE q.sistema_origem = %(sistema)s
                   AND q.entidade = 'venda_item'
                   AND (%(desde)s::date IS NULL
                        OR sv.dt_venda IS NULL
                        OR sv.dt_venda >= %(desde)s::date)
                 GROUP BY 1, 2
              ) t
             GROUP BY 1, 2
        """,
        sql_orfas_na_quarentena="""
            SELECT count(*) FROM trt.quarentena q
             WHERE q.sistema_origem = %(sistema)s AND q.entidade = 'venda_item'
               AND NOT EXISTS (SELECT 1 FROM stg.venda_item si
                                WHERE si.sistema_origem = q.sistema_origem
                                  AND si.id_item::text = q.chave)
        """),

    Verificacao(
        nome="estoque_mov",
        descricao_valor="qtd",
        sql_origem="""
            SELECT m.dt_mov::date AS dia, m.cod_loja::int AS cod_loja,
                   count(*)::bigint AS qtd,
                   coalesce(sum(m.qtd), 0) AS valor
              FROM erp.mov_estoque m
             WHERE %(desde)s::date IS NULL
                OR m.dt_mov IS NULL
                OR m.dt_mov >= %(desde)s::date
             GROUP BY 1, 2
        """,
        sql_destino="""
            SELECT dia, cod_loja,
                   sum(qtd_destino)::bigint     AS qtd_destino,
                   sum(vl_destino)              AS vl_destino,
                   sum(qtd_quarentena)::bigint  AS qtd_quarentena,
                   sum(vl_quarentena)           AS vl_quarentena
              FROM (
                SELECT t.dt_mov::date AS dia, t.cod_loja::int AS cod_loja,
                       count(*) AS qtd_destino, coalesce(sum(t.qtd), 0) AS vl_destino,
                       0 AS qtd_quarentena, 0::numeric AS vl_quarentena
                  FROM trt.estoque_mov t
                 WHERE t.sistema_origem = %(sistema)s
                   AND (%(desde)s::date IS NULL OR t.dt_mov >= %(desde)s::date)
                 GROUP BY 1, 2
                UNION ALL
                SELECT se.dt_mov::date, se.cod_loja::int,
                       0, 0::numeric,
                       count(*), coalesce(sum(se.qtd), 0)
                  FROM trt.quarentena q
                  JOIN stg.estoque_mov se ON se.sistema_origem = q.sistema_origem
                                         AND se.id_mov::text = q.chave
                 WHERE q.sistema_origem = %(sistema)s
                   AND q.entidade = 'estoque_mov'
                   AND (%(desde)s::date IS NULL
                        OR se.dt_mov IS NULL
                        OR se.dt_mov >= %(desde)s::date)
                 GROUP BY 1, 2
              ) t
             GROUP BY 1, 2
        """,
        sql_orfas_na_quarentena="""
            SELECT count(*) FROM trt.quarentena q
             WHERE q.sistema_origem = %(sistema)s AND q.entidade = 'estoque_mov'
               AND NOT EXISTS (SELECT 1 FROM stg.estoque_mov se
                                WHERE se.sistema_origem = q.sistema_origem
                                  AND se.id_mov::text = q.chave)
        """),
)


@dataclass
class Divergencia:
    verificacao: str
    dia: date | None
    cod_loja: int | None
    qtd_origem: int
    qtd_destino: int
    qtd_quarentena: int
    dif_qtd: int
    dif_valor: Decimal

    def __str__(self) -> str:
        onde = (f"{self.dia or 'sem dia'} loja "
                f"{self.cod_loja if self.cod_loja is not None else 'nula'}")
        return (f"{self.verificacao} {onde}: origem {self.qtd_origem}, "
                f"destino {self.qtd_destino} + quarentena {self.qtd_quarentena} "
                f"(falta {self.dif_qtd} linha(s), {self.dif_valor} em valor)")


@dataclass
class ResultadoConferencia:
    verificacao: str
    grupos: int = 0
    grupos_divergentes: int = 0
    dif_qtd_total: int = 0
    dif_valor_total: Decimal = Decimal(0)
    divergencias: list[Divergencia] = field(default_factory=list)

    @property
    def bateu(self) -> bool:
        return self.grupos_divergentes == 0


def _agregar(conn: psycopg.Connection, sql: str, parametros: dict) -> dict:
    """Roda a agregacao e devolve {(dia, cod_loja): tupla de medidas}."""
    with conn.cursor() as cur:
        cur.execute(sql, parametros)
        return {(linha[0], linha[1]): linha[2:] for linha in cur.fetchall()}


def conferir_uma(conn_origem: psycopg.Connection, conn_dw: psycopg.Connection,
                 verificacao: Verificacao, execucao: Execucao,
                 desde: date | None) -> ResultadoConferencia:
    """Compara um lado com o outro e grava o resultado em ctl.conferencia."""
    parametros = {"sistema": execucao.sistema_origem, "desde": desde}

    # A agregacao roda DENTRO de cada banco; so o resumo por dia e loja viaja
    # pela rede. Na escala media sao ~480 linhas, nao 1 milhao.
    origem = _agregar(conn_origem, verificacao.sql_origem, parametros)
    destino = _agregar(conn_dw, verificacao.sql_destino, parametros)

    # Guarda de invariante: toda linha na quarentena tem que ter a linha dela
    # no staging, senao o valor dela nao entra na conta e a conferencia
    # passaria sem cobrir aquela linha.
    with conn_dw.cursor() as cur:
        cur.execute(verificacao.sql_orfas_na_quarentena, parametros)
        orfas = cur.fetchone()[0]
    if orfas:
        raise ConferenciaFalhou(
            f"{verificacao.nome}: {orfas} linha(s) na quarentena sem linha "
            "correspondente no staging. A quarentena guarda referencia, nao "
            "copia: sem a linha no staging o valor dela nao entra na conta.")

    resultado = ResultadoConferencia(verificacao.nome)
    linhas_ctl = []

    for chave in sorted(set(origem) | set(destino),
                        key=lambda c: (c[0] or date.min, c[1] or -1)):
        dia, cod_loja = chave
        qtd_origem, vl_origem = origem.get(chave, (0, Decimal(0)))
        qtd_destino, vl_destino, qtd_quarentena, vl_quarentena = \
            destino.get(chave, (0, Decimal(0), 0, Decimal(0)))

        dif_qtd = qtd_origem - (qtd_destino + qtd_quarentena)
        dif_valor = vl_origem - (vl_destino + vl_quarentena)
        bateu = dif_qtd == 0 and abs(dif_valor) <= TOLERANCIA_VALOR

        resultado.grupos += 1
        if not bateu:
            resultado.grupos_divergentes += 1
            resultado.dif_qtd_total += dif_qtd
            resultado.dif_valor_total += dif_valor
            resultado.divergencias.append(Divergencia(
                verificacao.nome, dia, cod_loja, qtd_origem, qtd_destino,
                qtd_quarentena, dif_qtd, dif_valor))

        linhas_ctl.append((
            execucao.id_execucao, execucao.sistema_origem, verificacao.nome,
            dia, cod_loja, qtd_origem, vl_origem, qtd_destino, vl_destino,
            qtd_quarentena, vl_quarentena, dif_qtd, dif_valor, bateu))

    # O diagnostico vai pela conexao de REGISTRO, nao pela de dados. A leitura
    # acima roda dentro da transacao de publicacao (e por isso enxerga a
    # estrela que ainda nao foi comitada); a escrita aqui nao pode, porque o
    # rollback dessa transacao apagaria justamente a evidencia de qual dia x
    # loja divergiu. Ver controle.py, "DUAS CONEXOES".
    with execucao.log.cursor() as cur:
        cur.executemany(
            "INSERT INTO ctl.conferencia (id_execucao, sistema_origem, "
            "    verificacao, dia, cod_loja, qtd_origem, vl_origem, "
            "    qtd_destino, vl_destino, qtd_quarentena, vl_quarentena, "
            "    dif_qtd, dif_valor, bateu) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            linhas_ctl)
    if execucao.conn_log is None:
        conn_dw.commit()
    return resultado


def conferir(conn_origem: psycopg.Connection, conn_dw: psycopg.Connection,
             execucao: Execucao,
             desde: date | None = None) -> list[ResultadoConferencia]:
    """Roda todas as verificacoes. Levanta ConferenciaFalhou se alguma nao bate."""
    resultados = []
    for verificacao in VERIFICACOES:
        with execucao.cronometrar("conferencia", verificacao.nome) as marca:
            resultado = conferir_uma(conn_origem, conn_dw, verificacao,
                                     execucao, desde)
            marca["lidas"] = resultado.grupos
            marca["escritas"] = resultado.grupos_divergentes
        resultados.append(resultado)

    falhas = [r for r in resultados if not r.bateu]
    if falhas:
        raise ConferenciaFalhou(montar_mensagem(falhas))
    return resultados


def montar_mensagem(falhas: list[ResultadoConferencia]) -> str:
    """Mensagem de falha que diz o que fazer, nao so que deu errado."""
    linhas = ["a conferencia nao fechou: origem != destino + quarentena"]
    for resultado in falhas:
        linhas.append(
            f"  {resultado.verificacao}: {resultado.grupos_divergentes} de "
            f"{resultado.grupos} grupos (dia x loja) divergem; "
            f"faltam {resultado.dif_qtd_total} linha(s) e "
            f"{resultado.dif_valor_total} em valor")
        for divergencia in resultado.divergencias[:MAX_DIVERGENCIAS_NA_MENSAGEM]:
            linhas.append(f"    {divergencia}")
        resto = len(resultado.divergencias) - MAX_DIVERGENCIAS_NA_MENSAGEM
        if resto > 0:
            linhas.append(f"    ... e mais {resto} grupo(s). "
                          "Veja ctl.conferencia WHERE NOT bateu.")
    linhas.append(
        "  A marca-d'agua NAO foi avancada. Se a causa for linha que o ERP "
        "gravou com dt_alt fora da janela de reprocessamento, o incremental "
        "nao alcanca ela: rode 'python -m pipeline.carga --completa'.")
    return "\n".join(linhas)
