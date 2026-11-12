"""
Testes da carga (etapa 3), contra o Postgres.

ATENCAO: recriam o conteudo de erp_origem E de dw_varejo (ver tests/conftest.py).

O que se prova aqui:
  1. carga completa: nenhuma linha some. origem == estrela + quarentena.
  2. idempotencia: rodar de novo nao duplica nem muda nada.
  3. incremental: cancelamento so no cabecalho chega na estrela; chegada tardia
     DENTRO da janela entra; chegada tardia FORA da janela NAO entra - e esse
     buraco e exatamente do tamanho das linhas atrasadas, nada a mais.
  4. quarentena nao e permanente: o item rejeitado por produto inexistente
     entra na estrela sozinho quando o cadastro chega.
  5. a marca-d'agua NAO avanca quando a carga falha.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from conftest import DATA_REF, ESCALA_TESTE, gerar_delta, gerar_origem
from pipeline import carga, conector as mod_conector, controle, estrela

pytestmark = pytest.mark.banco

SISTEMA = "erp_a"


def um(conn, sql, parametros=()):
    return conn.execute(sql, parametros).fetchone()[0]


@pytest.fixture(scope="module")
def conector():
    return mod_conector.carregar(SISTEMA)


@pytest.fixture(scope="module")
def completa(conector, conexao_dw):
    """Origem na escala de teste + uma carga completa."""
    gerar_origem()
    assert carga.executar(conector, completa=True) == 0
    return conector


@pytest.fixture(scope="module")
def incremental(conector, conexao_dw):
    """Carga completa, um dia de operacao, carga incremental.

    Refaz tudo do zero para nao depender da ordem dos testes.

    O delta injeta chegada tardia com 15 dias de atraso e a janela de
    reprocessamento e de 7, entao a conferencia da historia inteira derruba
    esta carga. Os testes abaixo olham o DW DEPOIS dela, e desde que a
    publicacao virou atomica uma carga reprovada nao deixa nada publicado -
    o DW ficaria igual ao da carga completa e os testes nao provariam nada.

    Por isso o incremental roda com --conferir-desde depois do dia atrasado:
    a conferencia passa, a carga PUBLICA, e o DW fica no estado que interessa
    aqui - com as vendas novas, com o cancelamento de cabecalho, e com o
    buraco do atraso fora da janela. O proprio --conferir-desde e a troca
    documentada em conferencia.py, e tests/test_conferencia.py prova os dois
    lados dela: sem ele a carga reprova, com ele o furo passa.
    """
    gerar_origem()
    assert carga.executar(conector, completa=True) == 0
    gabarito = gerar_delta()
    depois_do_atraso = DATA_REF - timedelta(days=7)
    assert carga.executar(conector, completa=False,
                          conferir_desde=depois_do_atraso) == 0
    return gabarito


# ---------------------------------------------------------------------------
# 1. Carga completa: nada some
# ---------------------------------------------------------------------------


def test_carga_completa_nao_perde_nenhum_item(completa, conexao, conexao_dw):
    """A regra central do desenho: origem == estrela + quarentena."""
    origem = um(conexao, "SELECT count(*) FROM erp.mov_venda_item")
    fato = um(conexao_dw, "SELECT count(*) FROM dw.fato_venda_item "
                          "WHERE sistema_origem = %s", (SISTEMA,))
    quarentena = um(conexao_dw, "SELECT count(*) FROM trt.quarentena "
                                "WHERE sistema_origem = %s AND entidade = 'venda_item'",
                    (SISTEMA,))
    assert fato + quarentena == origem


def test_carga_completa_nao_perde_nenhuma_venda(completa, conexao, conexao_dw):
    origem = um(conexao, "SELECT count(*) FROM erp.mov_venda")
    tratadas = um(conexao_dw, "SELECT count(*) FROM trt.venda "
                              "WHERE sistema_origem = %s", (SISTEMA,))
    quarentena = um(conexao_dw, "SELECT count(*) FROM trt.quarentena "
                                "WHERE sistema_origem = %s AND entidade = 'venda'",
                    (SISTEMA,))
    assert tratadas + quarentena == origem


# Conferencia por dia e loja: contagem E soma. A quarentena guarda referencia,
# nao valores, entao o valor da linha rejeitada vem do staging por join - que e
# exatamente a razao de o staging ser persistente.
#
# ATENCAO ao sinal: nao da para supor "estrela <= origem". Item com qtd
# negativa tem vl_total negativo, e tirar ele da estrela AUMENTA a soma. Por
# isso a conferencia e igualdade com a quarentena somada, nunca desigualdade.
SQL_CONFERENCIA_ORIGEM = """
    SELECT v.dt_venda::date, v.cod_loja, count(*), coalesce(sum(i.vl_total), 0)
      FROM erp.mov_venda_item i
      JOIN erp.mov_venda v ON v.id_venda = i.id_venda
     WHERE v.cod_loja IS NOT NULL AND v.dt_venda IS NOT NULL
     GROUP BY 1, 2
"""

SQL_CONFERENCIA_DESTINO = """
    SELECT dia, cod_loja, sum(n)::bigint, sum(valor) FROM (
        SELECT c.data AS dia, l.cod_loja, count(*) AS n,
               coalesce(sum(f.vl_liquido), 0) AS valor
          FROM dw.fato_venda_item f
          JOIN dw.dim_calendario c ON c.sk_data = f.sk_data
          JOIN dw.dim_loja l       ON l.sk_loja = f.sk_loja
         WHERE f.sistema_origem = %(s)s
         GROUP BY 1, 2
        UNION ALL
        SELECT sv.dt_venda::date, sv.cod_loja, count(*),
               coalesce(sum(si.vl_total), 0)
          FROM trt.quarentena q
          JOIN stg.venda_item si ON si.sistema_origem = q.sistema_origem
                                AND si.id_item::text = q.chave
          JOIN stg.venda sv      ON sv.sistema_origem = si.sistema_origem
                                AND sv.id_venda = si.id_venda
         WHERE q.sistema_origem = %(s)s AND q.entidade = 'venda_item'
           AND sv.cod_loja IS NOT NULL AND sv.dt_venda IS NOT NULL
         GROUP BY 1, 2
    ) t GROUP BY 1, 2
"""


def test_conferencia_por_dia_e_loja_fecha_em_contagem_e_valor(completa, conexao,
                                                              conexao_dw):
    """origem == estrela + quarentena, dia a dia e loja a loja.

    Esta e a regra que a etapa 4 vai transformar em falha de carga. Contagem
    fechando nao basta: o valor tambem tem que fechar.
    """
    origem = {(d, loja): (n, v) for d, loja, n, v
              in conexao.execute(SQL_CONFERENCIA_ORIGEM).fetchall()}
    destino = {(d, loja): (n, v) for d, loja, n, v
               in conexao_dw.execute(SQL_CONFERENCIA_DESTINO,
                                     {"s": SISTEMA}).fetchall()}
    assert origem, "a origem esta vazia: o teste nao provaria nada"
    assert set(destino) == set(origem)
    divergentes = {chave: (origem[chave], destino[chave])
                   for chave in origem if origem[chave] != destino[chave]}
    assert not divergentes


def test_item_sem_cabecalho_fica_so_na_quarentena(completa, conexao, conexao_dw):
    """Orfao nao tem dia nem loja, entao fica fora da conferencia por dia.

    Ele precisa estar contado em algum lugar, e esse lugar e a quarentena.
    """
    orfaos = um(conexao, "SELECT count(*) FROM erp.mov_venda_item i "
                         " LEFT JOIN erp.mov_venda v ON v.id_venda = i.id_venda "
                         " WHERE v.id_venda IS NULL")
    assert orfaos > 0
    assert um(conexao_dw,
              "SELECT count(*) FROM trt.quarentena "
              " WHERE entidade = 'venda_item' AND motivo = 'venda_inexistente'"
              ) == orfaos


@pytest.mark.parametrize("motivo", ["venda_inexistente", "produto_inexistente",
                                    "qtd_invalida", "total_divergente",
                                    "cabecalho_invalido"])
def test_cada_motivo_de_quarentena_aparece(completa, conexao_dw, motivo):
    assert um(conexao_dw,
              "SELECT count(*) FROM trt.quarentena WHERE entidade = 'venda_item' "
              "AND motivo = %s", (motivo,)) > 0


def test_cabecalho_invalido_derruba_os_itens_dele(completa, conexao_dw):
    """Item de venda reprovada nao entra na estrela, mesmo sendo um item valido."""
    vendas_ruins = um(conexao_dw, "SELECT count(*) FROM trt.quarentena "
                                  "WHERE entidade = 'venda'")
    assert vendas_ruins > 0
    assert um(conexao_dw, """
        SELECT count(*) FROM dw.fato_venda_item f
         WHERE f.sistema_origem = %s AND f.id_venda::text IN (
               SELECT chave FROM trt.quarentena WHERE entidade = 'venda')""",
              (SISTEMA,)) == 0


def test_fato_de_estoque_e_denso(completa, conexao_dw):
    """Uma linha por dia x loja x produto desde o 1o movimento do par.

    E o que permite medir ruptura: dia sem movimento tambem precisa existir.
    """
    linhas = um(conexao_dw, "SELECT count(*) FROM dw.fato_estoque_dia "
                            "WHERE sistema_origem = %s", (SISTEMA,))
    esperado = um(conexao_dw, """
        SELECT coalesce(sum(dias), 0) FROM (
            SELECT (SELECT max(dt_mov::date) FROM trt.estoque_mov
                     WHERE sistema_origem = %(s)s) - min(dt_mov::date) + 1 AS dias
              FROM trt.estoque_mov WHERE sistema_origem = %(s)s
             GROUP BY cod_loja, cod_produto) p""", {"s": SISTEMA})
    assert linhas == esperado


def test_ruptura_e_saldo_nao_positivo(completa, conexao_dw):
    assert um(conexao_dw, "SELECT count(*) FROM dw.fato_estoque_dia "
                          "WHERE sistema_origem = %s "
                          "AND em_ruptura <> (qtd_saldo_final <= 0)",
              (SISTEMA,)) == 0


def test_fato_so_aponta_para_dimensao_existente(completa, conexao_dw):
    """Sem linha 'desconhecido': quem nao tem dimensao ficou na quarentena."""
    assert um(conexao_dw, """
        SELECT count(*) FROM dw.fato_venda_item f
         LEFT JOIN dw.dim_loja l ON l.sk_loja = f.sk_loja
         LEFT JOIN dw.dim_produto p ON p.sk_produto = f.sk_produto
         LEFT JOIN dw.dim_calendario c ON c.sk_data = f.sk_data
         WHERE l.sk_loja IS NULL OR p.sk_produto IS NULL OR c.sk_data IS NULL""") == 0


def test_cancelado_fica_na_estrela_e_nao_conta_como_receita(completa, conexao_dw):
    """fato_venda_item MANTEM cancelado: cancelamento vira metrica, nao sumico."""
    cancelados = um(conexao_dw, "SELECT count(*) FROM dw.fato_venda_item "
                                "WHERE sistema_origem = %s AND cancelado", (SISTEMA,))
    assert cancelados > 0
    receita_total = um(conexao_dw, "SELECT sum(vl_liquido) FROM dw.fato_venda_item "
                                   "WHERE sistema_origem = %s", (SISTEMA,))
    receita_valida = um(conexao_dw, "SELECT sum(vl_liquido) FROM dw.fato_venda_item "
                                    "WHERE sistema_origem = %s AND NOT cancelado",
                        (SISTEMA,))
    assert receita_valida < receita_total


# ---------------------------------------------------------------------------
# 2. Idempotencia
# ---------------------------------------------------------------------------


def _impressao_digital(conn) -> tuple:
    return (
        um(conn, "SELECT count(*) FROM dw.fato_venda_item"),
        um(conn, "SELECT coalesce(sum(vl_liquido), 0) FROM dw.fato_venda_item"),
        um(conn, "SELECT count(*) FROM dw.fato_estoque_dia"),
        um(conn, "SELECT coalesce(sum(qtd_saldo_final), 0) FROM dw.fato_estoque_dia"),
        um(conn, "SELECT count(*) FROM dw.dim_produto"),
        um(conn, "SELECT count(*) FROM trt.quarentena"),
    )


def test_carga_completa_repetida_nao_muda_nada(completa, conexao_dw):
    antes = _impressao_digital(conexao_dw)
    assert carga.executar(completa, completa=True) == 0
    assert _impressao_digital(conexao_dw) == antes


def test_incremental_sem_novidade_nao_muda_nada(completa, conexao_dw):
    """Rodar incremental duas vezes seguidas e inofensivo: tudo e upsert."""
    assert carga.executar(completa, completa=False) == 0
    antes = _impressao_digital(conexao_dw)
    assert carga.executar(completa, completa=False) == 0
    assert _impressao_digital(conexao_dw) == antes


# ---------------------------------------------------------------------------
# 3. Incremental: os casos temporais
# ---------------------------------------------------------------------------


def test_incremental_traz_as_vendas_novas_do_dia(incremental, conexao_dw):
    ids = incremental.injecoes["venda_nova_do_dia"]["ids"]
    assert um(conexao_dw, "SELECT count(DISTINCT id_venda) FROM dw.fato_venda_item "
                          "WHERE id_venda = ANY(%s)", (ids,)) == len(ids)


def test_incremental_traz_o_cancelamento_feito_so_no_cabecalho(incremental,
                                                               conexao_dw):
    """A armadilha principal do ERP: o item nao foi tocado, o dt_alt dele e antigo.

    Quem puxa isso e a clausula reextrair_quando_pai_mudar do conector.
    """
    ids = incremental.injecoes["venda_cancelada_cabecalho"]["ids"]
    total = um(conexao_dw, "SELECT count(*) FROM dw.fato_venda_item "
                           "WHERE id_venda = ANY(%s)", (ids,))
    cancelados = um(conexao_dw, "SELECT count(*) FROM dw.fato_venda_item "
                                "WHERE id_venda = ANY(%s) AND cancelado", (ids,))
    assert total > 0
    assert cancelados == total


def test_incremental_recupera_chegada_tardia_dentro_da_janela(incremental,
                                                              conexao_dw):
    ids = incremental.injecoes["chegada_tardia_dentro_janela"]["ids"]
    assert um(conexao_dw, "SELECT count(DISTINCT id_venda) FROM dw.fato_venda_item "
                          "WHERE id_venda = ANY(%s)", (ids,)) == len(ids)


def test_incremental_nao_alcanca_chegada_tardia_fora_da_janela(incremental,
                                                               conexao_dw):
    """Tem que ficar de fora MESMO. E a linha que a etapa 4 precisa denunciar."""
    ids = incremental.injecoes["chegada_tardia_fora_janela"]["ids"]
    assert ids
    assert um(conexao_dw, "SELECT count(*) FROM dw.fato_venda_item "
                          "WHERE id_venda = ANY(%s)", (ids,)) == 0


def test_o_buraco_do_incremental_e_exatamente_a_chegada_tardia_fora_da_janela(
        incremental, conexao, conexao_dw):
    """Nada alem do atraso deliberado escapa. Se escapar, o desenho tem furo."""
    ids = incremental.injecoes["chegada_tardia_fora_janela"]["ids"]
    origem = um(conexao, "SELECT count(*) FROM erp.mov_venda_item")
    fora_da_janela = um(conexao, "SELECT count(*) FROM erp.mov_venda_item "
                                 "WHERE id_venda = ANY(%s)", (ids,))
    fato = um(conexao_dw, "SELECT count(*) FROM dw.fato_venda_item "
                          "WHERE sistema_origem = %s", (SISTEMA,))
    quarentena = um(conexao_dw, "SELECT count(*) FROM trt.quarentena "
                                "WHERE sistema_origem = %s AND entidade = 'venda_item'",
                    (SISTEMA,))
    assert fora_da_janela > 0
    assert origem - (fato + quarentena) == fora_da_janela


def test_carga_completa_recupera_o_que_o_incremental_deixou_para_tras(
        incremental, conector, conexao, conexao_dw):
    """A carga completa ignora marca-d'agua: e a saida quando a conferencia falha."""
    assert carga.executar(conector, completa=True) == 0
    origem = um(conexao, "SELECT count(*) FROM erp.mov_venda_item")
    fato = um(conexao_dw, "SELECT count(*) FROM dw.fato_venda_item "
                          "WHERE sistema_origem = %s", (SISTEMA,))
    quarentena = um(conexao_dw, "SELECT count(*) FROM trt.quarentena "
                                "WHERE sistema_origem = %s AND entidade = 'venda_item'",
                    (SISTEMA,))
    assert fato + quarentena == origem


# ---------------------------------------------------------------------------
# 4. Quarentena nao e permanente
# ---------------------------------------------------------------------------


def test_item_sai_da_quarentena_quando_o_cadastro_que_faltava_chega(
        conector, conexao, conexao_dw):
    """Dimensao que chega tarde nao pode virar rejeicao permanente.

    Cenario: o item aponta para um produto que ainda nao foi cadastrado. Ele e
    rejeitado. Na carga seguinte o cadastro chega e o item entra sozinho na
    estrela, sem ninguem reprocessar nada a mao.
    """
    gerar_origem()
    assert carga.executar(conector, completa=True) == 0

    chave, cod_produto = conexao_dw.execute(
        "SELECT q.chave, s.cod_produto FROM trt.quarentena q "
        "  JOIN stg.venda_item s ON s.sistema_origem = q.sistema_origem "
        "                       AND s.id_item::text = q.chave "
        " WHERE q.entidade = 'venda_item' AND q.motivo = 'produto_inexistente' "
        " LIMIT 1").fetchone()
    id_item = int(chave)
    assert um(conexao_dw, "SELECT count(*) FROM dw.fato_venda_item "
                          "WHERE id_item = %s", (id_item,)) == 0

    # O cadastro atrasado finalmente chega na origem.
    conexao.execute(
        "INSERT INTO erp.cad_produto (cod_produto, descricao, unid_medida, "
        "  preco_venda, custo_medio, dt_alt) "
        "VALUES (%s, 'PRODUTO QUE CHEGOU ATRASADO', 'UN', 10.00, 6.00, "
        "        localtimestamp)", (cod_produto,))
    try:
        assert carga.executar(conector, completa=False) == 0
        assert um(conexao_dw, "SELECT count(*) FROM trt.quarentena "
                              "WHERE entidade = 'venda_item' AND chave = %s",
                  (chave,)) == 0
        assert um(conexao_dw, "SELECT count(*) FROM dw.fato_venda_item "
                              "WHERE id_item = %s", (id_item,)) == 1
    finally:
        conexao.execute("DELETE FROM erp.cad_produto WHERE cod_produto = %s",
                        (cod_produto,))


def test_quarentena_conta_as_tentativas(conector, conexao_dw):
    """Util para achar a linha que esta presa ha semanas."""
    assert um(conexao_dw, "SELECT count(*) FROM trt.quarentena "
                          "WHERE tentativas > 1") > 0


# ---------------------------------------------------------------------------
# 5. Marca-d'agua
# ---------------------------------------------------------------------------


def test_marca_dagua_avanca_depois_da_carga(conector, conexao_dw):
    gerar_origem()
    assert carga.executar(conector, completa=True) == 0
    for entidade in conector.entidades:
        assert controle.ler_marca_dagua(conexao_dw, SISTEMA, entidade) is not None


def test_marca_dagua_nao_avanca_quando_a_carga_falha(conector, conexao_dw,
                                                     monkeypatch):
    """Avancar a marca numa carga que falhou perderia dado em silencio."""
    gerar_origem()
    assert carga.executar(conector, completa=True) == 0
    antes = {e: controle.ler_marca_dagua(conexao_dw, SISTEMA, e)
             for e in conector.entidades}

    gerar_delta()

    def explodir(*_args, **_kwargs):
        raise RuntimeError("falha proposital no meio da carga")

    monkeypatch.setattr(estrela, "carregar", explodir)
    assert carga.executar(conector, completa=False) == 1

    depois = {e: controle.ler_marca_dagua(conexao_dw, SISTEMA, e)
              for e in conector.entidades}
    assert depois == antes

    status = um(conexao_dw, "SELECT status FROM ctl.execucao "
                            "ORDER BY id_execucao DESC LIMIT 1")
    assert status == "FALHA"


def test_incremental_le_a_janela_de_reprocessamento_e_nao_so_a_marca(
        conector, conexao_dw):
    """O corte da leitura tem que ficar N dias ATRAS da marca."""
    marca = controle.ler_marca_dagua(conexao_dw, SISTEMA, "venda_item")
    corte = controle.corte_da_leitura(conexao_dw, SISTEMA, "venda_item",
                                      conector.janela_dias)
    assert marca - corte == timedelta(days=conector.janela_dias)


def test_sem_marca_dagua_o_incremental_le_tudo(conexao_dw):
    assert controle.corte_da_leitura(conexao_dw, "erp_inexistente",
                                     "venda_item", 7) is None


# ---------------------------------------------------------------------------
# 6. Calendario
# ---------------------------------------------------------------------------


def test_calendario_cobre_todo_o_periodo_dos_fatos(conector, conexao_dw):
    gerar_origem()
    assert carga.executar(conector, completa=True) == 0
    intervalo = estrela.intervalo_tratado(conexao_dw, SISTEMA)
    assert intervalo is not None
    inicio, fim = intervalo
    faltando = um(conexao_dw,
                  "SELECT count(*) FROM generate_series(%s::date, %s::date, "
                  "   interval '1 day') d "
                  " WHERE NOT EXISTS (SELECT 1 FROM dw.dim_calendario c "
                  "                    WHERE c.data = d::date)",
                  (inicio, fim))
    assert faltando == 0


def test_calendario_marca_feriado_nacional(conexao_dw):
    assert um(conexao_dw, "SELECT eh_feriado_nacional FROM dw.dim_calendario "
                          "WHERE data = DATE '2026-11-15'") is True
    assert um(conexao_dw, "SELECT nome_feriado FROM dw.dim_calendario "
                          "WHERE data = DATE '2026-04-03'") == "Sexta-feira Santa"
    # Carnaval nao e feriado nacional, mas tem nome: o varejo precisa dele.
    assert um(conexao_dw, "SELECT eh_feriado_nacional FROM dw.dim_calendario "
                          "WHERE data = DATE '2026-02-17'") is False
    assert um(conexao_dw, "SELECT nome_feriado FROM dw.dim_calendario "
                          "WHERE data = DATE '2026-02-17'").startswith("Carnaval")
