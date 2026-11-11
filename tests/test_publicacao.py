"""
Publicacao atomica: ou a carga inteira aparece no DW, ou nada dela aparece.

ATENCAO: recriam o conteudo de erp_origem E de dw_varejo (ver tests/conftest.py).

O que se prova aqui, e por que cada um importa:

  1. Conferencia que nao fecha NAO deixa carga parcial visivel. Ate aqui o
     README prometia "nada publicado" e o codigo publicava: a estrela comitava
     por etapa, antes da conferencia. Quem consultasse o DW entre a carga e o
     conserto veria numero incompleto sem nada indicando isso.

  2. --completa que falha preserva o DW ANTERIOR. Ela e a saida que a propria
     mensagem de falha recomenda ao operador; se ela limpa primeiro e so depois
     recarrega, recomendar isso as 3 da manha e recomendar ficar sem DW.

  3. Linha gravada na origem DURANTE a carga nao derruba a conferencia. A
     extracao e a conferencia liam a origem em transacoes diferentes, entao uma
     venda que o ERP gravasse no meio da carga aparecia na contagem da origem
     sem estar no destino: falso positivo, carga reprovada sem motivo.

  4. O diagnostico sobrevive ao rollback. Se a publicacao vira uma transacao
     so, as linhas de ctl.conferencia da execucao que falhou seriam desfeitas
     junto - e elas sao a evidencia de QUAL dia divergiu. Tem que ser gravado
     por fora da transacao de publicacao.
"""

from __future__ import annotations

import pytest
from psycopg import sql

from conftest import DATA_REF, gerar_delta, gerar_origem
from pipeline import carga, conferencia, conector as mod_conector
from pipeline.conferencia import ConferenciaFalhou

pytestmark = pytest.mark.banco

SISTEMA = "erp_a"

# Tipos que entram na soma da impressao digital.
TIPOS_NUMERICOS = ("smallint", "integer", "bigint", "numeric",
                   "real", "double precision")


def um(conn, consulta, parametros=()):
    return conn.execute(consulta, parametros).fetchone()[0]


def impressao_do_dw(conn) -> dict[str, tuple]:
    """count(*) e a soma de toda coluna numerica, por tabela do schema dw.

    Montada a partir do catalogo e nao de uma lista fixa de proposito: tabela
    ou coluna nova entra na impressao sozinha, e um teste que deveria pegar
    "o DW mudou" nao fica cego porque alguem esqueceu de atualizar a lista.
    """
    tabelas = [linha[0] for linha in conn.execute(
        "SELECT table_name FROM information_schema.tables "
        " WHERE table_schema = 'dw' AND table_type = 'BASE TABLE' "
        " ORDER BY table_name").fetchall()]

    impressao: dict[str, tuple] = {}
    for tabela in tabelas:
        colunas = [linha[0] for linha in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            " WHERE table_schema = 'dw' AND table_name = %s "
            "   AND data_type = ANY(%s) "
            " ORDER BY ordinal_position", (tabela, list(TIPOS_NUMERICOS))).fetchall()]

        medidas = [sql.SQL("count(*)")]
        medidas += [sql.SQL("coalesce(sum({col}), 0)").format(col=sql.Identifier(c))
                    for c in colunas]
        consulta = sql.SQL("SELECT {medidas} FROM {tabela}").format(
            medidas=sql.SQL(", ").join(medidas),
            tabela=sql.Identifier("dw", tabela))
        impressao[tabela] = tuple(conn.execute(consulta).fetchone())
    return impressao


def marcas_dagua(conn, sistema: str = SISTEMA) -> list[tuple]:
    return conn.execute(
        "SELECT entidade, dt_alt_max, id_execucao FROM ctl.marca_dagua "
        " WHERE sistema_origem = %s ORDER BY entidade", (sistema,)).fetchall()


@pytest.fixture(scope="module")
def conector():
    return mod_conector.carregar(SISTEMA)


@pytest.fixture(scope="module")
def incremental_que_falhou(conector, conexao_dw):
    """Completa OK, um dia de operacao com atraso de 15 dias, incremental.

    O incremental DEVE falhar: a janela de reprocessamento e de 7 dias. A
    impressao do DW e tirada ANTES e DEPOIS, no mesmo ponto do fluxo, para
    que a comparacao signifique alguma coisa.
    """
    gerar_origem()
    assert carga.executar(conector, completa=True) == 0

    antes = impressao_do_dw(conexao_dw)
    gerar_delta()
    codigo = carga.executar(conector, completa=False)
    depois = impressao_do_dw(conexao_dw)
    id_execucao = um(conexao_dw, "SELECT max(id_execucao) FROM ctl.execucao")
    return antes, codigo, depois, id_execucao


# ---------------------------------------------------------------------------
# 1. Conferencia que nao fecha nao publica nada
# ---------------------------------------------------------------------------


def test_conferencia_falha_nao_deixa_nada_publicado_no_dw(
        incremental_que_falhou):
    """Carga que nao fecha a conta nao pode deixar meia carga visivel."""
    antes, codigo, depois, _id = incremental_que_falhou
    assert codigo == 1
    assert depois == antes


# ---------------------------------------------------------------------------
# 2. --completa que falha preserva o DW anterior
# ---------------------------------------------------------------------------


def test_completa_que_falha_na_conferencia_preserva_o_dw_anterior(
        conector, conexao_dw, monkeypatch):
    """A saida recomendada ao operador nao pode ser a que destroi o DW."""
    gerar_origem()
    assert carga.executar(conector, completa=True) == 0

    antes = impressao_do_dw(conexao_dw)
    marcas_antes = marcas_dagua(conexao_dw)
    assert antes["fato_venda_item"][0] > 0
    assert marcas_antes

    def nao_fecha(*_args, **_kwargs):
        raise ConferenciaFalhou("falha proposital na conferencia")

    monkeypatch.setattr(conferencia, "conferir", nao_fecha)
    assert carga.executar(conector, completa=True) == 1

    assert impressao_do_dw(conexao_dw) == antes
    assert marcas_dagua(conexao_dw) == marcas_antes


# ---------------------------------------------------------------------------
# 3. Escrita na origem durante a carga nao vira falso positivo
# ---------------------------------------------------------------------------


def _venda_nova_na_origem() -> int:
    """Grava uma venda com dt_alt = agora, como o ERP faria no meio da carga.

    Conexao propria, como erp_app, com commit: e uma escrita de FORA do
    pipeline, que e exatamente o caso que se quer reproduzir.
    """
    from pipeline.db import conectar_origem_app

    with conectar_origem_app() as conn:
        cod_loja, cod_produto, preco, custo = conn.execute(
            "SELECT (SELECT min(cod_loja) FROM erp.cad_loja), "
            "       p.cod_produto, p.preco_venda, p.custo_medio "
            "  FROM erp.cad_produto p ORDER BY p.cod_produto LIMIT 1").fetchone()

        id_venda = conn.execute(
            "INSERT INTO erp.mov_venda (cod_loja, nr_caixa, nr_cupom, dt_venda, "
            "    status, vl_bruto, vl_desconto, vl_total, forma_pagto) "
            "VALUES (%s, 1, 999999, %s, 'F', %s, 0, %s, 'PIX') "
            "RETURNING id_venda",
            (cod_loja, DATA_REF, preco, preco)).fetchone()[0]

        conn.execute(
            "INSERT INTO erp.mov_venda_item (id_venda, nr_item, cod_produto, "
            "    qtd, vl_unit, vl_desconto, vl_total, vl_custo_unit, status_item) "
            "VALUES (%s, 1, %s, 1, %s, 0, %s, %s, 'N')",
            (id_venda, cod_produto, preco, preco, custo))
        conn.commit()
    return id_venda


def test_linha_gravada_na_origem_durante_a_carga_nao_derruba_a_conferencia(
        conector, conexao_dw, monkeypatch):
    """O ERP nao para enquanto o pipeline roda.

    A venda e gravada DEPOIS da extracao e ANTES da conferencia. Se os dois
    lados nao olharem o mesmo retrato da origem, a conferencia acusa uma
    diferenca que nao existe - e a carga reprova sem ter perdido nada.
    """
    from pipeline import tratamento

    gerar_origem()
    assert carga.executar(conector, completa=True) == 0

    original = tratamento.tratar
    gravadas: list[int] = []

    def tratar_depois_de_escrever_na_origem(*args, **kwargs):
        if not gravadas:                      # so na primeira carga
            gravadas.append(_venda_nova_na_origem())
        return original(*args, **kwargs)

    monkeypatch.setattr(tratamento, "tratar",
                        tratar_depois_de_escrever_na_origem)
    assert carga.executar(conector, completa=False) == 0
    assert gravadas, "a venda de teste nao chegou a ser gravada"

    monkeypatch.undo()
    assert carga.executar(conector, completa=False) == 0
    assert um(conexao_dw, "SELECT count(*) FROM dw.fato_venda_item "
                          " WHERE id_venda = %s", (gravadas[0],)) == 1


# ---------------------------------------------------------------------------
# 4. O diagnostico sobrevive ao rollback
# ---------------------------------------------------------------------------


def test_diagnostico_da_conferencia_sobrevive_ao_rollback(
        incremental_que_falhou, conexao_dw):
    """ctl.conferencia e ctl.execucao sao a evidencia, nao parte do dado.

    Se forem desfeitas junto com a publicacao, a carga falha sem dizer QUAL
    dia divergiu - e dois testes de test_conferencia.py dependem dessas linhas.
    """
    _antes, codigo, _depois, id_execucao = incremental_que_falhou
    assert codigo == 1

    divergentes = um(conexao_dw,
                     "SELECT count(*) FROM ctl.conferencia "
                     " WHERE id_execucao = %s AND NOT bateu", (id_execucao,))
    assert divergentes > 0

    assert um(conexao_dw, "SELECT status FROM ctl.execucao "
                          " WHERE id_execucao = %s", (id_execucao,)) == "FALHA"
