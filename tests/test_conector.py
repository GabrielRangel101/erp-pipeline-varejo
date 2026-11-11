"""
Testes do conector YAML. Nao precisam de banco.

O que importa provar aqui: o YAML do ERP A e valido, um YAML errado falha com
mensagem que diz ONDE, e o SELECT incremental de venda_item inclui a clausula
que recupera o filho cujo pai mudou (sem ela, cancelamento de venda nunca
chegaria ao DW).
"""

from __future__ import annotations

from datetime import datetime

import pytest
import yaml

from pipeline import conector as mod
from pipeline.conector import ConectorInvalido

CORTE = datetime(2026, 11, 4, 0, 0)


def test_conector_do_erp_a_carrega():
    c = mod.carregar("erp_a")
    assert c.sistema_origem == "erp_a"
    assert c.schema == "erp"
    assert c.janela_dias == 7
    assert set(c.entidades) == {"loja", "fornecedor", "produto", "venda",
                                "venda_item", "estoque_mov"}


def test_toda_entidade_tem_chave_e_coluna_de_alteracao_dentro_de_colunas():
    for nome, entidade in mod.carregar("erp_a").entidades.items():
        assert entidade.chave, nome
        for coluna in entidade.chave:
            assert coluna in entidade.colunas, f"{nome}.{coluna}"
        assert entidade.coluna_alteracao in entidade.colunas, nome


def test_entidades_do_yaml_batem_com_as_tabelas_do_staging():
    """Nome canonico da entidade e nome da tabela em stg tem que ser o mesmo."""
    from pipeline.tratamento import REGRAS
    entidades = set(mod.carregar("erp_a").entidades)
    tratadas = {r.entidade for r in REGRAS}
    # fornecedor e estagiado mas nao tratado: vira atributo da dim_produto.
    assert tratadas == entidades - {"fornecedor"}


def test_carga_completa_le_a_tabela_inteira():
    c = mod.carregar("erp_a")
    consulta, parametros = c.entidade("loja").sql_extracao(c.schema, None)
    texto = consulta.as_string(None)
    assert "WHERE" not in texto
    assert '"erp"."cad_loja"' in texto
    assert len(parametros) == 2          # sistema_origem e id_execucao


def test_incremental_filtra_pela_coluna_de_alteracao():
    c = mod.carregar("erp_a")
    consulta, parametros = c.entidade("loja").sql_extracao(c.schema, CORTE)
    texto = consulta.as_string(None)
    assert 't."dt_alt" >= %s' in texto
    assert parametros[2:] == [CORTE]


def test_incremental_de_item_tambem_puxa_quem_teve_o_cabecalho_alterado():
    """A armadilha do cancelamento: o item fica com dt_alt antigo.

    Sem este OR, cancelar uma venda no ERP nunca chegaria na estrela.
    """
    c = mod.carregar("erp_a")
    consulta, parametros = c.entidade("venda_item").sql_extracao(c.schema, CORTE)
    texto = consulta.as_string(None)
    assert 't."dt_alt" >= %s' in texto
    assert 't."id_venda" IN (SELECT p."id_venda" FROM "erp"."mov_venda" AS p' in texto
    assert parametros[2:] == [CORTE, CORTE]      # a janela entra duas vezes


def test_entidade_sem_pai_nao_ganha_subconsulta():
    c = mod.carregar("erp_a")
    texto = c.entidade("venda").sql_extracao(c.schema, CORTE)[0].as_string(None)
    assert "IN (SELECT" not in texto


def test_entidade_inexistente_da_erro_que_lista_as_que_existem():
    c = mod.carregar("erp_a")
    with pytest.raises(ConectorInvalido, match="nao existe"):
        c.entidade("nota_fiscal")


# ---------------------------------------------------------------------------
# YAML malformado
# ---------------------------------------------------------------------------

BASE = {
    "sistema_origem": "erp_x",
    "schema": "erp",
    "janela_reprocessamento_dias": 7,
    "entidades": {
        "loja": {
            "tabela": "cad_loja",
            "chave": ["cod_loja"],
            "coluna_alteracao": "dt_alt",
            "colunas": {"cod_loja": "cod_loja", "dt_alt": "dt_alt"},
        }
    },
}


def _escrever(tmp_path, dados):
    caminho = tmp_path / "erp_x.yaml"
    caminho.write_text(yaml.safe_dump(dados), encoding="utf-8")
    return caminho


def _sem(caminho_chaves, valor=None):
    """Copia BASE trocando (ou removendo, se valor is None) uma chave aninhada."""
    import copy
    dados = copy.deepcopy(BASE)
    alvo = dados
    for chave in caminho_chaves[:-1]:
        alvo = alvo[chave]
    if valor is None:
        alvo.pop(caminho_chaves[-1], None)
    else:
        alvo[caminho_chaves[-1]] = valor
    return dados


def test_yaml_valido_de_mentira_carrega(tmp_path):
    c = mod.carregar(_escrever(tmp_path, BASE))
    assert c.sistema_origem == "erp_x"


@pytest.mark.parametrize("dados, trecho", [
    (_sem(["sistema_origem"]), "sistema_origem"),
    (_sem(["schema"]), "schema"),
    (_sem(["janela_reprocessamento_dias"]), "janela_reprocessamento_dias"),
    (_sem(["janela_reprocessamento_dias"], -1), "janela_reprocessamento_dias"),
    (_sem(["entidades"], {}), "entidades"),
    (_sem(["entidades", "loja", "tabela"]), "tabela"),
    (_sem(["entidades", "loja", "colunas"], {}), "colunas"),
    (_sem(["entidades", "loja", "chave"], []), "chave"),
    # chave que nao esta no mapa de colunas
    (_sem(["entidades", "loja", "chave"], ["id_filial"]), "nao esta em colunas"),
    # coluna_alteracao que nao esta no mapa de colunas
    (_sem(["entidades", "loja", "coluna_alteracao"], "atualizado_em"),
     "nao esta em colunas"),
    # chave desconhecida: erro de digitacao no YAML nao pode passar calado
    (_sem(["entidades", "loja", "tablea"], "cad_loja"), "desconhecida"),
])
def test_yaml_malformado_diz_onde(tmp_path, dados, trecho):
    with pytest.raises(ConectorInvalido, match=trecho):
        mod.carregar(_escrever(tmp_path, dados))


@pytest.mark.parametrize("nome_ruim", [
    'cad_loja"; DROP TABLE erp.cad_loja; --',
    "Cad_Loja",            # Postgres dobra para minusculo: exigimos minusculo
    "cad loja",
    "1_loja",
])
def test_identificador_fora_do_padrao_e_recusado(tmp_path, nome_ruim):
    """Nome vindo do YAML vira identificador SQL: tem que ser validado."""
    with pytest.raises(ConectorInvalido, match="identificador valido"):
        mod.carregar(_escrever(tmp_path, _sem(["entidades", "loja", "tabela"],
                                              nome_ruim)))


def test_expressao_sql_e_permitida_e_entra_no_select(tmp_path):
    """A escotilha {expr: ...} existe para ERP cuja coluna nao vem pronta."""
    dados = _sem(["entidades", "loja", "colunas"],
                 {"cod_loja": "cod_loja",
                  "nome_loja": {"expr": "upper(nome_fantasia)"},
                  "dt_alt": "dt_alt"})
    c = mod.carregar(_escrever(tmp_path, dados))
    texto = c.entidade("loja").sql_extracao(c.schema, None)[0].as_string(None)
    assert "upper(nome_fantasia)" in texto


def test_expressao_malformada_e_recusada(tmp_path):
    dados = _sem(["entidades", "loja", "colunas"],
                 {"cod_loja": "cod_loja", "dt_alt": {"expression": "now()"}})
    with pytest.raises(ConectorInvalido, match="expr"):
        mod.carregar(_escrever(tmp_path, dados))


def test_coluna_de_alteracao_nao_pode_ser_expressao(tmp_path):
    """A marca-d'agua compara contra esta coluna; precisa ser indexavel."""
    dados = _sem(["entidades", "loja", "colunas"],
                 {"cod_loja": "cod_loja", "dt_alt": {"expr": "now()"}})
    c = mod.carregar(_escrever(tmp_path, dados))
    with pytest.raises(ConectorInvalido, match="nao pode ser uma expressao"):
        c.entidade("loja").sql_extracao(c.schema, CORTE)


def test_conector_inexistente_da_erro_claro():
    with pytest.raises(ConectorInvalido, match="nao encontrado"):
        mod.carregar("erp_que_nao_existe")
