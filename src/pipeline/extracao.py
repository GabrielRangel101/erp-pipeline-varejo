"""
Extracao: origem (erp_origem, como erp_ro) -> staging (dw_varejo, schema stg).

Sao DOIS bancos. O Postgres nao faz consulta entre bancos, entao a extracao
passa obrigatoriamente pela rede - igual seria com um ERP de verdade, e sem
chance de o pipeline "colar" na origem por acidente.

Como os bytes viajam:

    COPY (SELECT ...) TO STDOUT     na origem
            |  blocos de texto, sem virar objeto Python
            v
    COPY tmp_<entidade> FROM STDIN  no DW (tabela temporaria)
            |
            v
    INSERT ... SELECT ... ON CONFLICT DO UPDATE   no staging

O desvio pela temporaria existe porque COPY nao faz ON CONFLICT. O ganho de
nao desserializar 1 milhao de linhas em Python compensa com folga a copia
extra. Formato TEXTO e nao binario de proposito: binario exige que o tipo da
origem bata exatamente com o do destino, e o staging usa tipos mais largos
justamente para caber ERP com layout diferente.
"""

from __future__ import annotations

from datetime import datetime

import psycopg
from psycopg import sql

from pipeline import limpeza
from pipeline.conector import Conector, Entidade

TAMANHO_BLOCO = 1 << 20     # 1 MiB por bloco lido da origem


def _colunas_destino(entidade: Entidade) -> list[str]:
    """Ordem exata que o SELECT da extracao produz."""
    return ["sistema_origem", *entidade.canonicos, "id_execucao"]


def _sql_upsert(entidade: Entidade, temporaria: str) -> sql.Composed:
    colunas = _colunas_destino(entidade)
    pk = ["sistema_origem", *entidade.chave]
    atualizaveis = [c for c in colunas if c not in pk]

    atribuicoes = [sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(c))
                   for c in atualizaveis]
    atribuicoes.append(sql.SQL("dt_carga = now()"))

    return sql.SQL(
        "INSERT INTO {destino} ({colunas}) SELECT {colunas} FROM {temporaria} "
        "ON CONFLICT ({pk}) DO UPDATE SET {atribuicoes}").format(
        destino=sql.Identifier("stg", entidade.nome),
        colunas=sql.SQL(", ").join(map(sql.Identifier, colunas)),
        temporaria=sql.Identifier(temporaria),
        pk=sql.SQL(", ").join(map(sql.Identifier, pk)),
        atribuicoes=sql.SQL(", ").join(atribuicoes),
    )


def extrair(conn_origem: psycopg.Connection, conn_dw: psycopg.Connection,
            conector: Conector, entidade: Entidade, id_execucao: int,
            desde: datetime | None) -> tuple[int, int, datetime | None]:
    """Traz uma entidade para o staging.

    Devolve (linhas lidas, linhas escritas, maior dt_alt lido). O maior dt_alt
    vira a marca-d'agua - mas quem grava a marca e o orquestrador, e so no fim
    da carga inteira (ver controle.py).
    """
    consulta, parametros = entidade.sql_extracao(conector.schema, desde)
    parametros[0] = conector.sistema_origem
    parametros[1] = id_execucao

    temporaria = f"tmp_{entidade.nome}"
    colunas = _colunas_destino(entidade)
    cur_dw = conn_dw.cursor()

    cur_dw.execute(sql.SQL(
        "CREATE TEMP TABLE {temporaria} (LIKE {modelo} INCLUDING DEFAULTS) "
        "ON COMMIT DROP").format(
            temporaria=sql.Identifier(temporaria),
            modelo=sql.Identifier("stg", entidade.nome)))

    copia_saida = sql.SQL("COPY ({consulta}) TO STDOUT").format(consulta=consulta)
    copia_entrada = sql.SQL("COPY {temporaria} ({colunas}) FROM STDIN").format(
        temporaria=sql.Identifier(temporaria),
        colunas=sql.SQL(", ").join(map(sql.Identifier, colunas)))

    with conn_origem.cursor() as cur_origem:
        with cur_origem.copy(copia_saida, parametros) as leitura:
            with cur_dw.copy(copia_entrada) as escrita:
                for bloco in leitura:
                    escrita.write(bloco)

    coluna_alt = sql.Identifier(entidade.coluna_alteracao)
    lidas, dt_alt_max = cur_dw.execute(sql.SQL(
        "SELECT count(*), max({coluna_alt}) FROM {temporaria}").format(
            coluna_alt=coluna_alt,
            temporaria=sql.Identifier(temporaria))).fetchone()

    cur_dw.execute(_sql_upsert(entidade, temporaria))
    escritas = cur_dw.rowcount

    conn_dw.commit()        # fecha a transacao e derruba a temporaria
    return lidas, escritas, dt_alt_max


def limpar_staging(conn_dw: psycopg.Connection, conector: Conector) -> str:
    """Carga completa: apaga o staging deste sistema de origem antes de reler.

    So deste sistema: um DW com dois ERPs nao pode ter a carga completa de um
    apagando o staging do outro.
    """
    return limpeza.limpar(conn_dw, "stg", list(conector.entidades),
                          conector.sistema_origem)
