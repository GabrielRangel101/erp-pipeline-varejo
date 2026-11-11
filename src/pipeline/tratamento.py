"""
Camada tratada: stg -> trt, com quarentena.

Toda linha do staging sai por UMA de duas portas: vira linha tratada ou vira
linha de quarentena, com o codigo da regra que falhou. Nao existe terceira
porta - e isso que permite a conferencia da etapa 4 exigir
origem == estrela + quarentena. Linha sumir calado e o modo de falha que este
desenho recusa.

O QUE E REAVALIADO A CADA CARGA
  carga completa:  tudo que esta no staging daquele sistema de origem.
  incremental:     o que esta carga acabou de estagiar MAIS tudo que esta na
                   quarentena. A linha quarentenada e RELIDA do staging, nao
                   de uma copia: item rejeitado por "produto_inexistente"
                   porque o cadastro ainda nao tinha chegado passa sozinho na
                   carga seguinte. Dimensao que chega tarde nao pode virar
                   rejeicao permanente.

ORDEM IMPORTA. loja e produto sao julgados antes de venda, que e julgada antes
de venda_item: o item so e aceito se o cabecalho, a loja e o produto passaram.
Um cabecalho reprovado derruba os itens dele com motivo 'cabecalho_invalido',
nao com 'venda_inexistente' - a distincao separa "orfao de verdade" de "o pai
esta na quarentena".

LIMITE CONHECIDO: se um cadastro que ja estava valido passa a ser invalido, os
fatos que apontam para ele continuam na estrela, apontando para a ultima
versao boa da dimensao. A invalidacao nao cascateia para tras. A conferencia
continua fechando (a linha esta em exatamente um lugar), mas o conteudo fica
com a versao antiga.
"""

from __future__ import annotations

from dataclasses import dataclass

import psycopg

from pipeline import limpeza
from pipeline.controle import Execucao

DOMINIO_TP_MOV = "('INV', 'CMP', 'VND', 'DEV', 'AJT', 'PRD')"


@dataclass(frozen=True)
class Regra:
    """Como julgar uma entidade e onde colocar quem passou."""

    entidade: str
    pk: tuple[str, ...]          # PK de trt.<entidade>, fora sistema_origem
    colunas: tuple[str, ...]     # colunas de trt.<entidade> que o julgamento produz
    chave_expr: str              # expressao que vira trt.quarentena.chave
    julgamento: str              # SELECT com colunas + chave + motivo + detalhe


REGRAS: tuple[Regra, ...] = (

    Regra(
        entidade="loja",
        pk=("cod_loja",),
        colunas=("cod_loja", "nome_loja", "cidade", "uf", "formato",
                 "dt_abertura", "ativa", "dt_alt_origem"),
        chave_expr="s.cod_loja::text",
        julgamento="""
            SELECT
                s.sistema_origem,
                s.cod_loja::text                        AS chave,
                CASE WHEN s.nome_loja IS NULL OR btrim(s.nome_loja) = ''
                     THEN 'descricao_ausente' END       AS motivo,
                NULL::text                              AS detalhe,
                s.cod_loja,
                left(btrim(s.nome_loja), 80)            AS nome_loja,
                left(btrim(s.cidade), 60)               AS cidade,
                upper(left(btrim(s.uf), 2))             AS uf,
                upper(left(btrim(s.formato), 20))       AS formato,
                s.dt_abertura,
                -- flag nao informada num ERP legado quer dizer "ativa"
                coalesce(s.ativa, true)                 AS ativa,
                s.dt_alt                                AS dt_alt_origem
            FROM stg.loja s
            {candidatos}
        """),

    Regra(
        entidade="produto",
        pk=("cod_produto",),
        colunas=("cod_produto", "cod_barras", "descricao", "departamento",
                 "secao", "unid_medida", "cod_fornecedor", "nome_fornecedor",
                 "preco_venda", "custo_medio", "ativo", "dt_alt_origem"),
        chave_expr="s.cod_produto::text",
        julgamento="""
            SELECT
                s.sistema_origem,
                s.cod_produto::text                     AS chave,
                CASE WHEN s.descricao IS NULL OR btrim(s.descricao) = ''
                     THEN 'descricao_ausente' END       AS motivo,
                NULL::text                              AS detalhe,
                s.cod_produto,
                left(btrim(s.cod_barras), 14)           AS cod_barras,
                left(btrim(s.descricao), 120)           AS descricao,
                left(btrim(s.departamento), 40)         AS departamento,
                left(btrim(s.secao), 40)                AS secao,
                upper(left(btrim(s.unid_medida), 2))    AS unid_medida,
                s.cod_fornecedor,
                -- fornecedor ausente NAO reprova o produto: a fase 1 nao tem
                -- dim_fornecedor, o nome e so um atributo da dim_produto.
                left(btrim(f.razao_social), 120)        AS nome_fornecedor,
                round(s.preco_venda, 2)                 AS preco_venda,
                round(s.custo_medio, 4)                 AS custo_medio,
                coalesce(s.ativo, true)                 AS ativo,
                s.dt_alt                                AS dt_alt_origem
            FROM stg.produto s
            LEFT JOIN stg.fornecedor f
                   ON f.sistema_origem = s.sistema_origem
                  AND f.cod_fornecedor = s.cod_fornecedor
            {candidatos}
        """),

    Regra(
        entidade="venda",
        pk=("id_venda",),
        colunas=("id_venda", "cod_loja", "dt_venda", "cancelada", "dt_alt_origem"),
        chave_expr="s.id_venda::text",
        julgamento="""
            SELECT
                s.sistema_origem,
                s.id_venda::text                        AS chave,
                CASE
                    WHEN s.dt_venda IS NULL                 THEN 'data_invalida'
                    WHEN s.status IS NULL
                      OR upper(btrim(s.status)) NOT IN ('F', 'C')
                                                            THEN 'status_invalido'
                    WHEN s.cod_loja IS NULL OR l.cod_loja IS NULL
                                                            THEN 'loja_inexistente'
                END                                     AS motivo,
                CASE
                    WHEN s.status IS NOT NULL
                     AND upper(btrim(s.status)) NOT IN ('F', 'C')
                        THEN 'status=' || s.status
                    WHEN s.cod_loja IS NULL
                        THEN 'cod_loja nulo'
                    WHEN l.cod_loja IS NULL
                        THEN 'cod_loja=' || s.cod_loja
                END                                     AS detalhe,
                s.id_venda,
                s.cod_loja,
                s.dt_venda,
                (upper(btrim(s.status)) = 'C')          AS cancelada,
                s.dt_alt                                AS dt_alt_origem
            FROM stg.venda s
            LEFT JOIN trt.loja l
                   ON l.sistema_origem = s.sistema_origem
                  AND l.cod_loja = s.cod_loja
            {candidatos}
        """),

    Regra(
        entidade="venda_item",
        pk=("id_item",),
        colunas=("id_item", "id_venda", "cod_loja", "cod_produto", "dt_venda",
                 "qtd", "vl_unit", "vl_bruto", "vl_desconto", "vl_liquido",
                 "vl_custo", "vl_margem", "cancelado", "dt_alt_origem"),
        chave_expr="s.id_item::text",
        julgamento="""
            SELECT
                s.sistema_origem,
                s.id_item::text                         AS chave,
                CASE
                    -- orfao de verdade: nao existe cabecalho nem no staging
                    WHEN s.id_venda IS NULL OR sv.id_venda IS NULL
                                                            THEN 'venda_inexistente'
                    -- o cabecalho existe, mas esta na quarentena
                    WHEN v.id_venda IS NULL                 THEN 'cabecalho_invalido'
                    WHEN s.cod_produto IS NULL OR p.cod_produto IS NULL
                                                            THEN 'produto_inexistente'
                    WHEN s.status_item IS NULL
                      OR upper(btrim(s.status_item)) NOT IN ('N', 'C')
                                                            THEN 'status_invalido'
                    WHEN s.qtd IS NULL OR s.qtd <= 0        THEN 'qtd_invalida'
                    WHEN s.vl_unit IS NULL OR s.vl_unit < 0
                      OR s.vl_total IS NULL                 THEN 'valor_invalido'
                    WHEN abs(s.vl_total - (round(s.qtd * s.vl_unit, 2)
                                           - coalesce(s.vl_desconto, 0))) > 0.01
                                                            THEN 'total_divergente'
                END                                     AS motivo,
                CASE
                    WHEN s.id_venda IS NOT NULL AND sv.id_venda IS NULL
                        THEN 'id_venda=' || s.id_venda
                    WHEN s.cod_produto IS NOT NULL AND p.cod_produto IS NULL
                        THEN 'cod_produto=' || s.cod_produto
                    WHEN s.qtd IS NULL                      THEN 'qtd nula'
                    WHEN s.qtd <= 0                         THEN 'qtd=' || s.qtd
                END                                     AS detalhe,
                s.id_item,
                s.id_venda,
                v.cod_loja,
                s.cod_produto,
                v.dt_venda,
                round(s.qtd, 3)                         AS qtd,
                round(s.vl_unit, 2)                     AS vl_unit,
                round(s.qtd * s.vl_unit, 2)             AS vl_bruto,
                round(coalesce(s.vl_desconto, 0), 2)    AS vl_desconto,
                round(s.vl_total, 2)                    AS vl_liquido,
                round(s.qtd * coalesce(s.vl_custo_unit, 0), 2) AS vl_custo,
                round(s.vl_total, 2)
                    - round(s.qtd * coalesce(s.vl_custo_unit, 0), 2) AS vl_margem,
                -- cancelado pelo ITEM ou pelo CABECALHO. O ERP so marca o
                -- cabecalho; sem este OR o cancelamento sumiria da estrela.
                (v.cancelada OR upper(btrim(s.status_item)) = 'C') AS cancelado,
                greatest(s.dt_alt, v.dt_alt_origem)     AS dt_alt_origem
            FROM stg.venda_item s
            LEFT JOIN stg.venda sv
                   ON sv.sistema_origem = s.sistema_origem
                  AND sv.id_venda = s.id_venda
            LEFT JOIN trt.venda v
                   ON v.sistema_origem = s.sistema_origem
                  AND v.id_venda = s.id_venda
            LEFT JOIN trt.produto p
                   ON p.sistema_origem = s.sistema_origem
                  AND p.cod_produto = s.cod_produto
            {candidatos}
        """),

    Regra(
        entidade="estoque_mov",
        pk=("id_mov",),
        colunas=("id_mov", "cod_loja", "cod_produto", "dt_mov", "tp_mov",
                 "sentido", "qtd", "dt_alt_origem"),
        chave_expr="s.id_mov::text",
        julgamento="""
            SELECT
                s.sistema_origem,
                s.id_mov::text                          AS chave,
                CASE
                    WHEN s.dt_mov IS NULL                   THEN 'data_invalida'
                    WHEN s.cod_loja IS NULL OR l.cod_loja IS NULL
                                                            THEN 'loja_inexistente'
                    WHEN s.cod_produto IS NULL OR p.cod_produto IS NULL
                                                            THEN 'produto_inexistente'
                    WHEN upper(btrim(coalesce(s.tp_mov, '')))
                         NOT IN """ + DOMINIO_TP_MOV + """
                      OR upper(btrim(coalesce(s.sentido, ''))) NOT IN ('E', 'S')
                                                            THEN 'tipo_mov_invalido'
                    WHEN s.qtd IS NULL OR s.qtd <= 0        THEN 'qtd_invalida'
                END                                     AS motivo,
                NULL::text                              AS detalhe,
                s.id_mov,
                s.cod_loja,
                s.cod_produto,
                s.dt_mov,
                upper(btrim(s.tp_mov))                  AS tp_mov,
                upper(btrim(s.sentido))                 AS sentido,
                round(s.qtd, 3)                         AS qtd,
                s.dt_alt                                AS dt_alt_origem
            FROM stg.estoque_mov s
            LEFT JOIN trt.loja l
                   ON l.sistema_origem = s.sistema_origem
                  AND l.cod_loja = s.cod_loja
            LEFT JOIN trt.produto p
                   ON p.sistema_origem = s.sistema_origem
                  AND p.cod_produto = s.cod_produto
            {candidatos}
        """),
)


def _clausula_candidatos(regra: Regra, completa: bool) -> str:
    """Quem entra no julgamento desta carga."""
    if completa:
        return "WHERE s.sistema_origem = %(sistema)s"
    # Incremental: o que esta carga estagiou + o que esta na quarentena.
    # A segunda metade e o que faz a rejeicao nao ser permanente.
    return (
        "WHERE s.sistema_origem = %(sistema)s\n"
        "  AND (s.id_execucao = %(execucao)s\n"
        "       OR EXISTS (SELECT 1 FROM trt.quarentena q\n"
        "                   WHERE q.sistema_origem = s.sistema_origem\n"
        f"                     AND q.entidade = '{regra.entidade}'\n"
        f"                     AND q.chave = {regra.chave_expr}))"
    )


def _sql_promocao(regra: Regra) -> str:
    colunas = ", ".join(regra.colunas)
    pk = ", ".join(("sistema_origem", *regra.pk))
    atualizaveis = [c for c in regra.colunas if c not in regra.pk]
    atribuicoes = ", ".join(f"{c} = EXCLUDED.{c}" for c in atualizaveis)
    return (
        f"INSERT INTO trt.{regra.entidade} (sistema_origem, {colunas}, id_execucao)\n"
        f"SELECT sistema_origem, {colunas}, %(execucao)s FROM julgamento\n"
        f" WHERE motivo IS NULL\n"
        f"ON CONFLICT ({pk}) DO UPDATE SET {atribuicoes},\n"
        f"    id_execucao = EXCLUDED.id_execucao, dt_carga = now()"
    )


@dataclass
class ResultadoTratamento:
    entidade: str
    julgadas: int
    promovidas: int
    rejeitadas: int
    liberadas: int                 # sairam da quarentena nesta carga
    por_motivo: dict[str, int]


def tratar_entidade(conn: psycopg.Connection, execucao: Execucao, regra: Regra,
                    completa: bool) -> ResultadoTratamento:
    parametros = {"sistema": execucao.sistema_origem,
                  "execucao": execucao.id_execucao,
                  "entidade": regra.entidade}
    cur = conn.cursor()

    julgamento = regra.julgamento.format(
        candidatos=_clausula_candidatos(regra, completa))
    cur.execute(f"CREATE TEMP TABLE julgamento ON COMMIT DROP AS\n{julgamento}",
                parametros)
    julgadas = cur.rowcount

    # Quem estava na quarentena e passou agora. Materializado numa temporaria
    # pequena para o DELETE nao varrer o julgamento inteiro por linha.
    cur.execute(
        "CREATE TEMP TABLE liberados ON COMMIT DROP AS "
        "SELECT q.chave FROM trt.quarentena q "
        "JOIN julgamento j ON j.chave = q.chave AND j.motivo IS NULL "
        "WHERE q.sistema_origem = %(sistema)s AND q.entidade = %(entidade)s",
        parametros)
    liberadas = cur.rowcount

    cur.execute(_sql_promocao(regra), parametros)
    promovidas = cur.rowcount

    cur.execute(
        "INSERT INTO trt.quarentena (sistema_origem, entidade, chave, motivo, "
        "    detalhe, id_execucao_entrada, id_execucao_ultima) "
        "SELECT sistema_origem, %(entidade)s, chave, motivo, detalhe, "
        "       %(execucao)s, %(execucao)s "
        "  FROM julgamento WHERE motivo IS NOT NULL "
        "ON CONFLICT (sistema_origem, entidade, chave) DO UPDATE SET "
        "    motivo = EXCLUDED.motivo, "
        "    detalhe = EXCLUDED.detalhe, "
        "    id_execucao_ultima = EXCLUDED.id_execucao_ultima, "
        "    dt_ultima = now(), "
        "    tentativas = trt.quarentena.tentativas + 1",
        parametros)
    rejeitadas = cur.rowcount

    cur.execute(
        "DELETE FROM trt.quarentena q USING liberados l "
        " WHERE q.sistema_origem = %(sistema)s AND q.entidade = %(entidade)s "
        "   AND q.chave = l.chave",
        parametros)

    por_motivo = dict(cur.execute(
        "SELECT motivo, count(*) FROM julgamento "
        "WHERE motivo IS NOT NULL GROUP BY 1 ORDER BY 1").fetchall())

    conn.commit()          # derruba as temporarias
    # Mesma razao do ANALYZE em estrela.py, e tambem so na carga completa: a
    # tabela tratada acabou de ser truncada e repopulada, e o passo seguinte
    # faz join sobre ela.
    if completa:
        cur.execute(f"ANALYZE trt.{regra.entidade}")
        conn.commit()
    return ResultadoTratamento(regra.entidade, julgadas, promovidas,
                               rejeitadas, liberadas, por_motivo)


def tratar(conn: psycopg.Connection, execucao: Execucao,
           completa: bool) -> list[ResultadoTratamento]:
    """Julga todas as entidades, na ordem de dependencia de REGRAS."""
    resultados = []
    for regra in REGRAS:
        with execucao.cronometrar("tratamento", regra.entidade) as marca:
            resultado = tratar_entidade(conn, execucao, regra, completa)
            marca["lidas"] = resultado.julgadas
            marca["escritas"] = resultado.promovidas
        resultados.append(resultado)
    return resultados


def limpar_tratada(conn: psycopg.Connection, sistema_origem: str) -> str:
    """Carga completa: zera trt e a quarentena deste sistema antes de rejulgar."""
    return limpeza.limpar(conn, "trt",
                          [r.entidade for r in REGRAS] + ["quarentena"],
                          sistema_origem)
