"""Testes dos cadastros sinteticos. Nao precisam de banco."""

from __future__ import annotations

import re
from datetime import datetime
from random import Random

from pipeline import catalogo
from pipeline.gerador import ESCALAS, _distribuir_itens
from datetime import date, timedelta

DT = datetime(2026, 1, 1, 2, 0)


def _produtos(n: int = 300, semente: int = 7):
    return catalogo.gerar_produtos(n, 10, Random(semente), DT)


def test_ean13_digito_verificador():
    # Exemplos com digito conhecido, conferidos pelo algoritmo oficial.
    assert catalogo.digito_ean13("400638133393") == 1
    assert catalogo.digito_ean13("978030640615") == 7
    for codigo in (p.cod_barras for p in _produtos()):
        assert len(codigo) == 13
        assert catalogo.digito_ean13(codigo[:12]) == int(codigo[12])


def test_cod_barras_usa_faixa_interna_200_299():
    """Prefixo GS1 200-299 e reservado a uso interno: nao colide com produto real."""
    for p in _produtos():
        assert 200 <= int(p.cod_barras[:3]) <= 299


def test_cadastros_sao_deterministicos_por_semente():
    assert _produtos(semente=11) == _produtos(semente=11)
    assert _produtos(semente=11) != _produtos(semente=12)
    a = catalogo.gerar_fornecedores(30, Random(5), DT)
    assert a == catalogo.gerar_fornecedores(30, Random(5), DT)


def test_sem_dado_pessoal_nem_cnpj():
    """Nada de CPF/CNPJ: um numero gerado poderia bater com o de empresa real."""
    cnpj = re.compile(r"\d{2}[.\s]?\d{3}[.\s]?\d{3}[/\s]?\d{4}[-\s]?\d{2}")
    cpf = re.compile(r"\d{3}[.\s]?\d{3}[.\s]?\d{3}[-\s]?\d{2}")
    for razao in (f[1] for f in catalogo.gerar_fornecedores(30, Random(3), DT)):
        assert not cnpj.search(razao) and not cpf.search(razao)
        assert not any(c.isdigit() for c in razao) or razao.split()[-1] == "LTDA"


def test_embalagem_combina_com_o_produto():
    """Arroz nao vem em litro: a embalagem sai do pool da secao, nao de um sorteio geral."""
    solidos = ("ARROZ", "FEIJAO", "ACUCAR", "MACARRAO")
    for p in _produtos(1500, semente=42):
        if p.descricao.startswith(solidos):
            assert "ML" not in p.descricao and "L " not in p.descricao, p.descricao
        if p.unid_medida == "KG":
            assert not any(ch.isdigit() for ch in p.descricao), p.descricao


def test_toda_secao_tem_pool_de_embalagem():
    """Secao de produto UN sem pool quebraria a geracao com KeyError."""
    for departamento, secoes in catalogo.SECOES.items():
        unid = catalogo.DEPARTAMENTOS[departamento][0]
        if unid != "UN":
            continue
        for secao in secoes:
            assert secao in catalogo.TAMANHO_POR_SECAO, f"{departamento}/{secao}"


def test_distribuicao_de_itens_bate_com_o_alvo():
    escala = ESCALAS["pequena"]
    dias = [date(2026, 11, 11) - timedelta(days=escala.dias - i)
            for i in range(escala.dias)]
    alvo = _distribuir_itens(escala, dias, catalogo.peso_das_lojas())
    assert len(alvo) == escala.dias * len(catalogo.LOJAS_BASE)
    total = sum(alvo.values())
    assert abs(total - escala.itens_alvo) / escala.itens_alvo < 0.01


def test_fim_de_semana_vende_mais_que_segunda():
    escala = ESCALAS["pequena"]
    dias = [date(2026, 11, 11) - timedelta(days=escala.dias - i)
            for i in range(escala.dias)]
    alvo = _distribuir_itens(escala, dias, catalogo.peso_das_lojas())
    sabados = [v for (d, _l), v in alvo.items() if d.weekday() == 5]
    segundas = [v for (d, _l), v in alvo.items() if d.weekday() == 0]
    assert sum(sabados) / len(sabados) > sum(segundas) / len(segundas)
