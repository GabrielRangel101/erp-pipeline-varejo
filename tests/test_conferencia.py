"""
Testes da conferencia (etapa 4), contra o Postgres.

ATENCAO: recriam o conteudo de erp_origem e de dw_varejo (ver tests/conftest.py).

O que se prova aqui, em ordem de importancia:

  1. A conferencia FECHA depois de uma carga completa, em contagem e em valor,
     dia a dia e loja a loja - inclusive no grupo sem dia e sem loja, onde
     caem os itens orfaos.
  2. A conferencia FALHA quando tem que falhar. Um teste que so sabe dizer
     "passou" nao prova nada: aqui a linha que o ERP gravou com dt_alt fora da
     janela de reprocessamento derruba a carga, com exit code != 0.
  3. Quando ela falha, a marca-d'agua NAO avanca e a execucao fica como FALHA.
  4. A carga completa recupera o que o incremental nao alcancou.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from conftest import DATA_REF, gerar_delta, gerar_origem
from pipeline import carga, conferencia, conector as mod_conector, controle
from pipeline.conferencia import ConferenciaFalhou

pytestmark = pytest.mark.banco

SISTEMA = "erp_a"


def um(conn, sql, parametros=()):
    return conn.execute(sql, parametros).fetchone()[0]


@pytest.fixture(scope="module")
def conector():
    return mod_conector.carregar(SISTEMA)


@pytest.fixture(scope="module")
def completa(conector, conexao_dw):
    gerar_origem()
    assert carga.executar(conector, completa=True) == 0
    return conector


@pytest.fixture(scope="module")
def incremental_que_falhou(conector, conexao_dw):
    """Carga completa, um dia de operacao com atraso de 15 dias, incremental.

    O incremental DEVE falhar: a janela de reprocessamento e de 7 dias.
    """
    gerar_origem()
    assert carga.executar(conector, completa=True) == 0
    gabarito = gerar_delta()
    codigo = carga.executar(conector, completa=False)
    return gabarito, codigo


# ---------------------------------------------------------------------------
# 1. Fecha quando tem que fechar
# ---------------------------------------------------------------------------


def test_carga_completa_passa_na_conferencia(completa):
    """Se isto falhar, nao adianta olhar mais nada."""
    assert carga.executar(completa, completa=True) == 0


@pytest.mark.parametrize("verificacao", ["venda_item", "estoque_mov"])
def test_toda_linha_conferida_bateu(completa, conexao_dw, verificacao):
    total = um(conexao_dw,
               "SELECT count(*) FROM ctl.conferencia "
               " WHERE id_execucao = (SELECT max(id_execucao) FROM ctl.conferencia) "
               "   AND verificacao = %s", (verificacao,))
    assert total > 0, "a conferencia nao gravou nada"
    assert um(conexao_dw,
              "SELECT count(*) FROM ctl.conferencia "
              " WHERE id_execucao = (SELECT max(id_execucao) FROM ctl.conferencia) "
              "   AND verificacao = %s AND NOT bateu", (verificacao,)) == 0


def test_conferencia_soma_origem_igual_destino_mais_quarentena(completa,
                                                               conexao_dw):
    """A conta inteira, nao so o veredito linha a linha."""
    linha = conexao_dw.execute(
        "SELECT sum(qtd_origem), sum(qtd_destino) + sum(qtd_quarentena), "
        "       sum(vl_origem), sum(vl_destino) + sum(vl_quarentena) "
        "  FROM ctl.conferencia "
        " WHERE id_execucao = (SELECT max(id_execucao) FROM ctl.conferencia) "
        "   AND verificacao = 'venda_item'").fetchone()
    assert linha[0] == linha[1]
    assert abs(linha[2] - linha[3]) <= conferencia.TOLERANCIA_VALOR


def test_item_orfao_cai_no_grupo_sem_dia_e_sem_loja(completa, conexao,
                                                    conexao_dw):
    """Orfao nao tem dia nem loja, mas precisa ser contado em algum lugar."""
    orfaos = um(conexao, "SELECT count(*) FROM erp.mov_venda_item i "
                         " LEFT JOIN erp.mov_venda v ON v.id_venda = i.id_venda "
                         " WHERE v.id_venda IS NULL")
    assert orfaos > 0
    linha = conexao_dw.execute(
        "SELECT qtd_origem, qtd_destino, qtd_quarentena, bateu "
        "  FROM ctl.conferencia "
        " WHERE id_execucao = (SELECT max(id_execucao) FROM ctl.conferencia) "
        "   AND verificacao = 'venda_item' AND dia IS NULL AND cod_loja IS NULL"
    ).fetchone()
    assert linha is not None, "o grupo sem dia e sem loja nao foi conferido"
    qtd_origem, qtd_destino, qtd_quarentena, bateu = linha
    assert qtd_origem == orfaos
    assert qtd_destino == 0           # orfao nunca entra na estrela
    assert qtd_quarentena == orfaos   # esta todo na quarentena
    assert bateu


def test_conferencia_grava_as_linhas_que_bateram_tambem(completa, conexao_dw):
    """Conferencia que so registra falha nao prova que chegou a rodar."""
    assert um(conexao_dw,
              "SELECT count(*) FROM ctl.conferencia "
              " WHERE id_execucao = (SELECT max(id_execucao) FROM ctl.conferencia) "
              "   AND bateu") > 0


# ---------------------------------------------------------------------------
# 2. Falha quando tem que falhar
# ---------------------------------------------------------------------------


def test_chegada_tardia_fora_da_janela_derruba_a_carga(incremental_que_falhou):
    """O teste mais importante do projeto.

    Sem ele, a conferencia poderia estar sempre dizendo "bate" e ninguem
    perceberia.
    """
    _gabarito, codigo = incremental_que_falhou
    assert codigo == 1, "a carga passou com linha faltando na estrela"


def test_a_falha_aponta_o_dia_certo(incremental_que_falhou, conexao_dw):
    """O dia divergente e exatamente o da chegada tardia fora da janela."""
    gabarito, _codigo = incremental_que_falhou
    dia_atrasado = DATA_REF - timedelta(days=15)
    dias = [r[0] for r in conexao_dw.execute(
        "SELECT DISTINCT dia FROM ctl.conferencia "
        " WHERE id_execucao = (SELECT max(id_execucao) FROM ctl.conferencia) "
        "   AND NOT bateu ORDER BY 1").fetchall()]
    assert dias == [dia_atrasado]
    assert gabarito.injecoes["chegada_tardia_fora_janela"]["qtd"] > 0


def test_o_tamanho_do_furo_e_exatamente_o_que_ficou_para_tras(
        incremental_que_falhou, conexao, conexao_dw):
    """Nem mais, nem menos: se divergir de outra coisa, o desenho tem furo."""
    gabarito, _codigo = incremental_que_falhou
    ids = gabarito.injecoes["chegada_tardia_fora_janela"]["ids"]
    itens_perdidos = um(conexao, "SELECT count(*) FROM erp.mov_venda_item "
                                 " WHERE id_venda = ANY(%s)", (ids,))
    faltando = um(conexao_dw,
                  "SELECT coalesce(sum(dif_qtd), 0) FROM ctl.conferencia "
                  " WHERE id_execucao = (SELECT max(id_execucao) FROM ctl.conferencia) "
                  "   AND verificacao = 'venda_item' AND NOT bateu")
    assert itens_perdidos > 0
    assert faltando == itens_perdidos


def test_falha_nao_avanca_a_marca_dagua(incremental_que_falhou, conexao_dw):
    """Avancar a marca depois de nao fechar a conta esconderia o problema."""
    ultima = um(conexao_dw, "SELECT max(id_execucao) FROM ctl.execucao")
    assert um(conexao_dw, "SELECT status FROM ctl.execucao WHERE id_execucao = %s",
              (ultima,)) == "FALHA"
    gravada_por = um(conexao_dw,
                     "SELECT max(id_execucao) FROM ctl.marca_dagua "
                     " WHERE sistema_origem = %s", (SISTEMA,))
    assert gravada_por < ultima


def test_repetir_o_incremental_continua_falhando(incremental_que_falhou,
                                                 conector):
    """Nao destrava sozinho. Erro alto e melhor que erro silencioso."""
    _gabarito, _codigo = incremental_que_falhou
    assert carga.executar(conector, completa=False) == 1


def test_carga_completa_recupera_e_a_conferencia_volta_a_fechar(
        incremental_que_falhou, conector, conexao_dw):
    """A saida documentada para o operador."""
    _gabarito, _codigo = incremental_que_falhou
    assert carga.executar(conector, completa=True) == 0
    assert um(conexao_dw,
              "SELECT count(*) FROM ctl.conferencia "
              " WHERE id_execucao = (SELECT max(id_execucao) FROM ctl.conferencia) "
              "   AND NOT bateu") == 0


def test_conferir_desde_depois_do_atraso_deixa_o_furo_passar(conector,
                                                             conexao_dw):
    """--conferir-desde e uma troca, e o teste deixa isso explicito.

    Limitar o periodo faz a carga passar com a linha atrasada faltando. E por
    isso que o padrao e conferir a historia inteira.
    """
    gerar_origem()
    assert carga.executar(conector, completa=True) == 0
    gerar_delta()
    assert carga.executar(conector, completa=False) == 1
    depois_do_atraso = DATA_REF - timedelta(days=7)
    assert carga.executar(conector, completa=False,
                          conferir_desde=depois_do_atraso) == 0


# ---------------------------------------------------------------------------
# 3. Invariantes da propria conferencia
# ---------------------------------------------------------------------------


def test_quarentena_sem_linha_no_staging_derruba_a_conferencia(
        completa, conexao_dw):
    """A quarentena guarda referencia, nao copia.

    Se a linha sumir do staging, o valor dela nao entra na conta e a
    conferencia passaria sem cobrir aquela linha. Isso tem que ser recusado.
    """
    execucao = controle.abrir_execucao(conexao_dw, SISTEMA, "incremental")
    conexao_dw.execute(
        "INSERT INTO trt.quarentena (sistema_origem, entidade, chave, motivo, "
        "    id_execucao_entrada, id_execucao_ultima) "
        "VALUES (%s, 'venda_item', 'id_que_nao_existe', 'teste', %s, %s)",
        (SISTEMA, execucao.id_execucao, execucao.id_execucao))
    try:
        from pipeline.db import conectar_origem_leitura
        with conectar_origem_leitura() as origem:
            with pytest.raises(ConferenciaFalhou, match="sem linha"):
                conferencia.conferir(origem, conexao_dw, execucao)
    finally:
        conexao_dw.execute("DELETE FROM trt.quarentena WHERE chave = %s",
                           ("id_que_nao_existe",))


def test_tolerancia_vale_so_para_valor_nao_para_contagem():
    """Um centavo de arredondamento pode passar; uma linha a menos, nunca."""
    assert conferencia.TOLERANCIA_VALOR > 0
    assert conferencia.TOLERANCIA_VALOR < 1


def test_mensagem_de_falha_diz_o_que_fazer():
    resultado = conferencia.ResultadoConferencia("venda_item", grupos=10,
                                                 grupos_divergentes=1,
                                                 dif_qtd_total=28)
    resultado.divergencias.append(conferencia.Divergencia(
        "venda_item", date(2026, 10, 27), 3, 305, 295, 1, 9, 0))
    mensagem = conferencia.montar_mensagem([resultado])
    assert "2026-10-27" in mensagem
    assert "--completa" in mensagem
    assert "marca-d'agua NAO foi avancada" in mensagem
