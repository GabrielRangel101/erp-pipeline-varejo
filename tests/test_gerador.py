"""
Testes do gerador contra o Postgres.

ATENCAO: recriam o conteudo de erp_origem (ver tests/conftest.py).

O que se prova aqui:
  1. mesma semente -> mesmos dados, byte a byte (md5 por tabela);
  2. o gabarito saida/gerador_resumo.json bate com a origem consultada em SQL;
  3. cada tipo de inconsistencia existe mesmo;
  4. o --delta produz os quatro casos temporais que o incremental precisa
     enfrentar: venda nova, alteracao tardia, chegada tardia DENTRO da janela
     de reprocessamento e chegada tardia FORA dela.
"""

from __future__ import annotations

from datetime import datetime, time, timedelta

import pytest

from conftest import DATA_REF, ESCALA_TESTE, SEMENTE, contar
from pipeline import gerador
from pipeline.gerador import Gabarito

pytestmark = pytest.mark.banco


class _Desfazer(Exception):
    """Sai de "with conexao.transaction()" desfazendo o que foi escrito."""

# Cada tipo de defeito e a consulta que o encontra na origem.
CONSULTAS_DEFEITO = {
    "item_orfao_venda":
        "SELECT count(*) FROM erp.mov_venda_item i "
        "LEFT JOIN erp.mov_venda v USING (id_venda) WHERE v.id_venda IS NULL",
    "item_orfao_produto":
        "SELECT count(*) FROM erp.mov_venda_item i "
        "LEFT JOIN erp.cad_produto p USING (cod_produto) WHERE p.cod_produto IS NULL",
    "item_qtd_nula":
        "SELECT count(*) FROM erp.mov_venda_item WHERE qtd IS NULL",
    "item_qtd_zero":
        "SELECT count(*) FROM erp.mov_venda_item WHERE qtd = 0",
    "item_qtd_negativa":
        "SELECT count(*) FROM erp.mov_venda_item WHERE qtd < 0",
    "item_total_divergente":
        "SELECT count(*) FROM erp.mov_venda_item WHERE qtd IS NOT NULL "
        "AND abs(vl_total - (round(qtd * vl_unit, 2) - vl_desconto)) > 0.01",
    "venda_status_invalido":
        "SELECT count(*) FROM erp.mov_venda WHERE status NOT IN ('F', 'C')",
    "venda_loja_nula":
        "SELECT count(*) FROM erp.mov_venda WHERE cod_loja IS NULL",
    "venda_cancelada_cabecalho":
        "SELECT count(*) FROM erp.mov_venda WHERE status = 'C'",
}


def _carga_inicial(conexao) -> Gabarito:
    gab = Gabarito()
    gerador.carga_inicial(conexao, ESCALA_TESTE, SEMENTE, DATA_REF, gab)
    return gab


def _md5_por_tabela(conexao) -> dict[str, str]:
    """Impressao digital de cada tabela, independente da ordem de leitura."""
    cur = conexao.cursor()
    saida = {}
    for tabela in gerador.TABELAS_ERP:
        cur.execute(
            f"SELECT md5(coalesce(string_agg(t::text, '|' ORDER BY t::text), '')) "
            f"FROM erp.{tabela} t")
        saida[tabela] = cur.fetchone()[0]
    return saida


@pytest.fixture(scope="module")
def inicial(conexao):
    gab = _carga_inicial(conexao)
    return gab, conexao


@pytest.fixture(scope="module")
def delta(conexao):
    """Carga inicial + um dia de operacao. Refaz a base para nao depender de ordem."""
    _carga_inicial(conexao)
    gab = Gabarito()
    gerador.carga_delta(conexao, SEMENTE + 1, DATA_REF, None, gab)
    return gab, conexao


# ---------------------------------------------------------------------------
# 1. Determinismo
# ---------------------------------------------------------------------------


def test_mesma_semente_gera_exatamente_os_mesmos_dados(conexao):
    _carga_inicial(conexao)
    primeira = _md5_por_tabela(conexao)
    _carga_inicial(conexao)
    assert _md5_por_tabela(conexao) == primeira


def test_semente_diferente_gera_dados_diferentes(conexao):
    _carga_inicial(conexao)
    primeira = _md5_por_tabela(conexao)
    gab = Gabarito()
    gerador.carga_inicial(conexao, ESCALA_TESTE, SEMENTE + 99, DATA_REF, gab)
    segunda = _md5_por_tabela(conexao)
    # Lojas sao fixas; todo o resto tem que mudar.
    assert segunda["cad_loja"] == primeira["cad_loja"]
    assert segunda["mov_venda"] != primeira["mov_venda"]
    assert segunda["cad_produto"] != primeira["cad_produto"]


# ---------------------------------------------------------------------------
# 2. Gabarito x origem
# ---------------------------------------------------------------------------


def test_contagem_por_tabela_bate_com_o_gabarito(inicial):
    gab, conexao = inicial
    for tabela, esperado in gab.inseridos.items():
        assert contar(conexao, f"SELECT count(*) FROM erp.{tabela}") == esperado, tabela


def test_volume_de_itens_fica_perto_do_alvo_da_escala(inicial):
    _gab, conexao = inicial
    itens = contar(conexao, "SELECT count(*) FROM erp.mov_venda_item")
    desvio = abs(itens - ESCALA_TESTE.itens_alvo) / ESCALA_TESTE.itens_alvo
    assert desvio < 0.05, f"{itens} itens para um alvo de {ESCALA_TESTE.itens_alvo}"


@pytest.mark.parametrize("tipo", sorted(CONSULTAS_DEFEITO))
def test_cada_defeito_existe_na_quantidade_exata_do_gabarito(inicial, tipo):
    gab, conexao = inicial
    esperado = gab.injecoes[tipo]["qtd"]
    assert esperado > 0, f"{tipo} nao foi injetado"
    assert contar(conexao, CONSULTAS_DEFEITO[tipo]) == esperado


def test_ids_do_gabarito_apontam_para_linhas_reais(inicial):
    gab, conexao = inicial
    for tipo, registro in gab.injecoes.items():
        ids = registro["ids"]
        if not ids:
            continue
        tabela, coluna = (("mov_venda", "id_venda") if tipo.startswith("venda_")
                          else ("mov_venda_item", "id_item"))
        achados = contar(
            conexao,
            f"SELECT count(*) FROM erp.{tabela} WHERE {coluna} = ANY(%s)", (ids,))
        assert achados == len(ids), tipo


def test_venda_cancelada_nao_toca_nos_itens(inicial):
    """Armadilha 3: cancelar mexe so no cabecalho; os itens ficam 'N'."""
    _gab, conexao = inicial
    assert contar(conexao,
                  "SELECT count(*) FROM erp.mov_venda_item i "
                  "JOIN erp.mov_venda v USING (id_venda) "
                  "WHERE v.status = 'C' AND i.status_item <> 'N'") == 0
    assert contar(conexao,
                  "SELECT count(*) FROM erp.mov_venda_item i "
                  "JOIN erp.mov_venda v USING (id_venda) "
                  "WHERE v.status = 'C'") > 0


def test_sequencias_ajustadas_apos_o_copy(inicial):
    """Sem setval, o primeiro INSERT depois da carga colidiria com os ids do COPY."""
    _gab, conexao = inicial
    maximo = contar(conexao, "SELECT max(id_venda) FROM erp.mov_venda")
    novo = None
    with pytest.raises(_Desfazer):
        with conexao.transaction():
            linha = conexao.execute(
                "INSERT INTO erp.mov_venda (cod_loja, dt_venda, status) "
                "VALUES (1, localtimestamp, 'F') RETURNING id_venda").fetchone()
            novo = linha[0]
            raise _Desfazer
    assert novo > maximo


def test_historico_termina_na_vespera_da_data_ref(inicial):
    _gab, conexao = inicial
    cur = conexao.cursor()
    cur.execute("SELECT min(dt_venda)::date, max(dt_venda)::date FROM erp.mov_venda")
    primeiro, ultimo = cur.fetchone()
    assert ultimo == DATA_REF - timedelta(days=1)
    assert primeiro == DATA_REF - timedelta(days=ESCALA_TESTE.dias)


def test_carga_inicial_nasce_toda_com_dt_alt_historico(inicial):
    """Nada de localtimestamp na carga inicial: senao ela nao seria reproduzivel."""
    _gab, conexao = inicial
    limite = datetime.combine(DATA_REF, time(0, 0))
    for tabela in gerador.TABELAS_ERP:
        assert contar(conexao,
                      f"SELECT count(*) FROM erp.{tabela} WHERE dt_alt >= %s",
                      (limite,)) == 0, tabela


# ---------------------------------------------------------------------------
# 3. Delta: os quatro casos temporais do incremental
# ---------------------------------------------------------------------------


def test_delta_grava_vendas_novas_do_dia(delta):
    gab, conexao = delta
    ids = gab.injecoes["venda_nova_do_dia"]["ids"]
    assert ids
    assert contar(conexao,
                  "SELECT count(*) FROM erp.mov_venda "
                  "WHERE id_venda = ANY(%s) AND dt_venda::date = %s AND dt_alt >= %s",
                  (ids, DATA_REF, datetime.combine(DATA_REF, time(0, 0)))) == len(ids)


def test_delta_alteracao_tardia_carimba_dt_alt_pela_trigger(delta):
    """Venda antiga alterada hoje: dt_venda continua antigo, dt_alt vira hoje."""
    gab, conexao = delta
    ids = gab.injecoes["atualizacao_tardia"]["ids"]
    assert len(ids) == gerador.DELTA_ATUALIZACAO_TARDIA
    assert contar(conexao,
                  "SELECT count(*) FROM erp.mov_venda "
                  "WHERE id_venda = ANY(%s) AND dt_venda < %s AND dt_alt >= %s",
                  (ids, datetime.combine(DATA_REF, time(0, 0)),
                   datetime.combine(DATA_REF, time(0, 0)))) == len(ids)


def test_delta_cancela_so_o_cabecalho_e_deixa_os_itens_para_tras(delta):
    """Extrair itens so por dt_alt perderia o cancelamento: o item nao foi tocado."""
    gab, conexao = delta
    ids = gab.injecoes["venda_cancelada_cabecalho"]["ids"]
    meia_noite = datetime.combine(DATA_REF, time(0, 0))
    assert len(ids) == gerador.DELTA_CANCELAMENTO
    assert contar(conexao,
                  "SELECT count(*) FROM erp.mov_venda "
                  "WHERE id_venda = ANY(%s) AND status = 'C' AND dt_alt >= %s",
                  (ids, meia_noite)) == len(ids)
    # Nenhum item dessas vendas foi alterado hoje.
    assert contar(conexao,
                  "SELECT count(*) FROM erp.mov_venda_item "
                  "WHERE id_venda = ANY(%s) AND (status_item <> 'N' OR dt_alt >= %s)",
                  (ids, meia_noite)) == 0
    assert contar(conexao,
                  "SELECT count(*) FROM erp.mov_venda_item WHERE id_venda = ANY(%s)",
                  (ids,)) > 0


def test_delta_chegada_tardia_dentro_da_janela_de_reprocessamento(delta):
    """dt_alt de 1 a 5 dias atras: fica abaixo da marca-d'agua, mas a janela N=7 pega."""
    gab, conexao = delta
    ids = gab.injecoes["chegada_tardia_dentro_janela"]["ids"]
    assert len(ids) == gerador.DELTA_TARDIA_DENTRO_JANELA
    piso = datetime.combine(DATA_REF - timedelta(
        days=gerador.JANELA_REPROCESSAMENTO_DIAS), time(0, 0))
    teto = datetime.combine(DATA_REF, time(0, 0))
    assert contar(conexao,
                  "SELECT count(*) FROM erp.mov_venda "
                  "WHERE id_venda = ANY(%s) AND dt_alt >= %s AND dt_alt < %s",
                  (ids, piso, teto)) == len(ids)


def test_delta_chegada_tardia_fora_da_janela_e_invisivel_ao_incremental(delta):
    """dt_alt de 15 dias atras: nem a marca-d'agua nem a janela N=7 alcancam.

    E de proposito: e esta linha que faz a conferencia da etapa 4 FALHAR.
    """
    gab, conexao = delta
    ids = gab.injecoes["chegada_tardia_fora_janela"]["ids"]
    assert len(ids) == gerador.DELTA_TARDIA_FORA_JANELA
    piso = datetime.combine(DATA_REF - timedelta(
        days=gerador.JANELA_REPROCESSAMENTO_DIAS), time(0, 0))
    assert contar(conexao,
                  "SELECT count(*) FROM erp.mov_venda "
                  "WHERE id_venda = ANY(%s) AND dt_alt < %s",
                  (ids, piso)) == len(ids)


def test_delta_injeta_todos_os_tipos_de_inconsistencia(delta):
    gab, _conexao = delta
    for tipo, esperado in gerador.DEFEITOS_DELTA.items():
        assert gab.injecoes[tipo]["qtd"] == esperado, tipo


def test_delta_exige_carga_inicial(conexao):
    with pytest.raises(_Desfazer):
        with conexao.transaction():
            conexao.execute("TRUNCATE "
                            + ", ".join("erp." + t for t in gerador.TABELAS_ERP))
            with pytest.raises(RuntimeError, match="carga inicial"):
                gerador.carga_delta(conexao, SEMENTE, DATA_REF, None, Gabarito())
            raise _Desfazer
