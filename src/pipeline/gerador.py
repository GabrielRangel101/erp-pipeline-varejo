"""
Gerador de dados sinteticos do ERP A (banco erp_origem, schema erp).

    python -m pipeline.gerador --escala pequena|media --semente 42
    python -m pipeline.gerador --delta --semente 43

Dois modos:

  CARGA INICIAL (--escala)  apaga a origem e recria cadastros + historico.
      Escreve tudo por COPY, com ids explicitos, e no fim ajusta as sequencias
      com setval. Toda linha ja nasce com dt_alt historico (nada de localtimestamp),
      entao a carga inicial e reproduzivel: mesma semente + mesma --data-ref
      produzem exatamente os mesmos dados.

  DELTA (--delta)           simula UM dia de operacao sobre a base existente.
      Usa UPDATE de verdade onde o ERP usaria, para que a trigger de dt_alt
      carimbe a hora atual, e INSERT com dt_alt retroativo onde o ERP legado
      gravaria uma linha atrasada (importacao, contingencia fiscal).

As "armadilhas" (ver cabecalho de sql/origem/01_schema_erp_a.sql) sao injetadas
em quantidade EXATA e registradas em saida/gerador_resumo.json. Esse arquivo e o
gabarito: a conferencia da etapa 4 sabe por ele quantas linhas ruins existem e
quais ids esperar na quarentena.

Convencao de datas: o historico cobre [data_ref - dias, data_ref - 1]. O modo
--delta escreve "hoje" = data_ref. Por padrao data_ref e a data de hoje.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, time as hora, timedelta
from decimal import ROUND_HALF_UP, Decimal
from itertools import accumulate
from pathlib import Path
from random import Random

import psycopg

from pipeline import catalogo
from pipeline.catalogo import CENTAVO, Produto
from pipeline.db import RAIZ, conectar_origem_app

# ---------------------------------------------------------------------------
# Parametros
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Escala:
    nome: str
    dias: int
    itens_alvo: int
    lojas: int = 4
    produtos: int = 1500
    fornecedores: int = 30


ESCALAS: dict[str, Escala] = {
    # ~50 mil itens: roda em CI em poucos segundos.
    "pequena": Escala("pequena", dias=30, itens_alvo=50_000),
    # ~1 milhao de itens: escala de medicao (CLAUDE.md).
    "media": Escala("media", dias=120, itens_alvo=1_000_000),
}

# Janela de reprocessamento do incremental (etapa 3). Fica no gabarito para a
# conferencia saber que chegada tardia acima disso DEVE falhar.
JANELA_REPROCESSAMENTO_DIAS = 7

# Movimento por dia da semana (0 = segunda). Sexta e sabado puxam o varejo alimentar.
PESO_DIA_SEMANA = [0.85, 0.85, 0.90, 0.95, 1.20, 1.45, 0.80]

FORMAS_PAGTO = ["DINHEIRO", "CREDITO", "DEBITO", "PIX", "VOUCHER"]
PESO_FORMA_PAGTO = [0.10, 0.28, 0.27, 0.30, 0.05]

# Itens por cupom (media ~4,6).
ITENS_POR_CUPOM = [1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15]
PESO_ITENS_POR_CUPOM = [8, 12, 14, 14, 12, 10, 8, 7, 7, 5, 3]

# random.choices(weights=...) refaz a soma acumulada a CADA chamada. Com 1.500
# produtos e centenas de milhares de cupons isso dominaria o tempo de geracao,
# entao as somas acumuladas sao calculadas uma vez e passadas em cum_weights.
CUM_FORMA_PAGTO = list(accumulate(PESO_FORMA_PAGTO))
CUM_ITENS_POR_CUPOM = list(accumulate(PESO_ITENS_POR_CUPOM))

PROB_CANCELAMENTO = 0.005     # ~0,5% dos cupons cancelados no cabecalho
PROB_PROMOCAO = 0.08          # itens com preco promocional
PROB_DESCONTO_ITEM = 0.12     # itens com desconto no cupom

# Defeitos de qualidade injetados na carga inicial (quantidade EXATA).
DEFEITOS_INICIAL = {
    "item_orfao_venda": 12,
    "item_orfao_produto": 12,
    "item_qtd_nula": 8,
    "item_qtd_zero": 8,
    "item_qtd_negativa": 8,
    "item_total_divergente": 20,
    "venda_status_invalido": 5,
    "venda_loja_nula": 5,
}

# Injecoes do modo --delta (quantidade EXATA).
DEFEITOS_DELTA = {
    "item_orfao_venda": 3,
    "item_orfao_produto": 3,
    "item_qtd_nula": 2,
    "item_qtd_zero": 2,
    "item_qtd_negativa": 2,
    "item_total_divergente": 5,
    "venda_status_invalido": 2,
    "venda_loja_nula": 2,
}
DELTA_ATUALIZACAO_TARDIA = 40       # UPDATE sem dt_alt => trigger carimba agora
DELTA_CANCELAMENTO = 30             # UPDATE so no cabecalho; itens intocados
DELTA_TARDIA_DENTRO_JANELA = 25     # INSERT com dt_alt de 1 a 5 dias atras
DELTA_TARDIA_FORA_JANELA = 6        # INSERT com dt_alt de 15 dias atras
DELTA_DIAS_FORA_JANELA = 15

# Ids que nao existem nos cadastros, usados nos orfaos.
BASE_VENDA_INEXISTENTE = 9_000_000_000
BASE_PRODUTO_INEXISTENTE = 900_000

DIVERGENCIA_VL_TOTAL = Decimal("9.99")   # bem acima da tolerancia de 0,01

TABELAS_ERP = [
    "cad_fornecedor", "cad_loja", "cad_produto", "mov_venda", "mov_venda_item",
    "mov_estoque", "mov_compra", "mov_compra_item",
]

SEQUENCIAS = [
    ("mov_venda", "id_venda"),
    ("mov_venda_item", "id_item"),
    ("mov_estoque", "id_mov"),
    ("mov_compra", "id_compra"),
    ("mov_compra_item", "id_compra_item"),
]

COLUNAS = {
    "cad_fornecedor": ("cod_fornecedor", "razao_social", "uf", "ativo",
                       "dt_cadastro", "dt_alt"),
    "cad_loja": ("cod_loja", "nome_loja", "cidade", "uf", "formato",
                 "dt_abertura", "ativa", "dt_alt"),
    "cad_produto": ("cod_produto", "cod_barras", "descricao", "departamento",
                    "secao", "unid_medida", "cod_fornecedor", "preco_venda",
                    "custo_medio", "ativo", "dt_alt"),
    "mov_venda": ("id_venda", "cod_loja", "nr_caixa", "nr_cupom", "dt_venda",
                  "status", "vl_bruto", "vl_desconto", "vl_total",
                  "forma_pagto", "dt_alt"),
    "mov_venda_item": ("id_item", "id_venda", "nr_item", "cod_produto", "qtd",
                       "vl_unit", "vl_desconto", "vl_total", "vl_custo_unit",
                       "status_item", "dt_alt"),
    "mov_estoque": ("id_mov", "cod_loja", "cod_produto", "dt_mov", "tp_mov",
                    "sentido", "qtd", "doc_origem", "dt_alt"),
    "mov_compra": ("id_compra", "cod_loja", "cod_fornecedor", "nr_nf",
                   "dt_emissao", "dt_entrada", "status", "vl_total", "dt_alt"),
    "mov_compra_item": ("id_compra_item", "id_compra", "cod_produto", "qtd",
                        "vl_custo_unit", "vl_total", "dt_alt"),
}


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------


class Contador:
    """Gerador de ids sequenciais explicitos (as PKs sao BY DEFAULT AS IDENTITY)."""

    def __init__(self, inicio: int = 1) -> None:
        self.valor = inicio - 1

    def proximo(self) -> int:
        self.valor += 1
        return self.valor


class Gabarito:
    """Acumula o que a execucao escreveu, para saida/gerador_resumo.json."""

    def __init__(self) -> None:
        self.inseridos: dict[str, int] = {}
        self.atualizados: dict[str, int] = {}
        self.injecoes: dict[str, dict] = {}

    def inseriu(self, tabela: str, n: int) -> None:
        if n:
            self.inseridos[tabela] = self.inseridos.get(tabela, 0) + n

    def atualizou(self, tabela: str, n: int) -> None:
        if n:
            self.atualizados[tabela] = self.atualizados.get(tabela, 0) + n

    def injetou(self, tipo: str, ids: list[int]) -> None:
        reg = self.injecoes.setdefault(tipo, {"qtd": 0, "ids": []})
        reg["qtd"] += len(ids)
        reg["ids"].extend(ids)


def copiar(cur: psycopg.Cursor, tabela: str, linhas: list[tuple]) -> int:
    """COPY em lote. Devolve quantas linhas foram escritas."""
    if not linhas:
        return 0
    colunas = ", ".join(COLUNAS[tabela])
    with cur.copy(f"COPY erp.{tabela} ({colunas}) FROM STDIN") as cp:
        for linha in linhas:
            cp.write_row(linha)
    return len(linhas)


def _cent(valor: Decimal) -> Decimal:
    return valor.quantize(CENTAVO, rounding=ROUND_HALF_UP)


def sortear_qtd(unid_medida: str, rng: Random) -> Decimal:
    """Quantidade vendida: inteira para UN, fracionada para KG."""
    if unid_medida == "KG":
        return Decimal(rng.randrange(150, 3500)).scaleb(-3)
    return Decimal(rng.choices([1, 1, 1, 2, 2, 3, 4, 6, 12], k=1)[0])


def _hora_do_movimento(dia: date, rng: Random) -> datetime:
    """Hora comercial, com pico no fim da tarde."""
    minuto_do_dia = int(rng.triangular(7 * 60, 22 * 60, 18 * 60))
    return datetime.combine(dia, hora(0, 0)) + timedelta(minutes=minuto_do_dia,
                                                         seconds=rng.randrange(60))


# ---------------------------------------------------------------------------
# Carga inicial
# ---------------------------------------------------------------------------


def _distribuir_itens(escala: Escala, dias: list[date],
                      pesos_loja: dict[int, float]) -> dict[tuple[date, int], int]:
    """Quantos itens cada par (dia, loja) deve produzir, somando ~itens_alvo."""
    bruto: dict[tuple[date, int], float] = {}
    for dia in dias:
        peso_dia = PESO_DIA_SEMANA[dia.weekday()]
        # Pico de inicio de mes (pagamento) e leve queda no fim.
        peso_dia *= 1.15 if dia.day <= 7 else (0.92 if dia.day >= 25 else 1.0)
        for cod_loja, peso_loja in pesos_loja.items():
            bruto[(dia, cod_loja)] = peso_dia * peso_loja
    total = sum(bruto.values())
    return {chave: max(1, round(escala.itens_alvo * peso / total))
            for chave, peso in bruto.items()}


def _pesos_popularidade(produtos: list[Produto], rng: Random) -> list[float]:
    """Lei de potencia acumulada: poucos produtos respondem por boa parte das vendas.

    Ja devolve cum_weights para o random.choices (ver CUM_ITENS_POR_CUPOM).
    """
    posicoes = list(range(1, len(produtos) + 1))
    rng.shuffle(posicoes)
    return list(accumulate(1.0 / (pos ** 0.8) for pos in posicoes))


def carga_inicial(conn: psycopg.Connection, escala: Escala, semente: int,
                  data_ref: date, gab: Gabarito) -> None:
    if escala.lojas != len(catalogo.LOJAS_BASE):
        raise ValueError(
            f"escala pede {escala.lojas} lojas, mas catalogo.LOJAS_BASE tem "
            f"{len(catalogo.LOJAS_BASE)}. Acerte os dois, senao o gabarito mente."
        )
    rng = Random(semente)
    cur = conn.cursor()

    cur.execute("SET search_path = erp, public")
    cur.execute(f"TRUNCATE {', '.join('erp.' + t for t in TABELAS_ERP)} RESTART IDENTITY")

    dias = [data_ref - timedelta(days=escala.dias - i) for i in range(escala.dias)]
    primeiro_dia = dias[0]
    dt_cadastro = datetime.combine(primeiro_dia - timedelta(days=1), hora(2, 0))

    # ---------------- cadastros ----------------
    gab.inseriu("cad_loja", copiar(cur, "cad_loja", catalogo.gerar_lojas(dt_cadastro)))
    gab.inseriu("cad_fornecedor", copiar(
        cur, "cad_fornecedor",
        catalogo.gerar_fornecedores(escala.fornecedores, rng, dt_cadastro)))

    produtos = catalogo.gerar_produtos(escala.produtos, escala.fornecedores,
                                       rng, dt_cadastro)
    gab.inseriu("cad_produto", copiar(cur, "cad_produto",
                                      catalogo.linhas_produto(produtos, dt_cadastro)))

    por_codigo = {p.cod_produto: p for p in produtos}
    codigos = [p.cod_produto for p in produtos]
    cum_pesos = _pesos_popularidade(produtos, rng)
    pesos_loja = catalogo.peso_das_lojas()
    cods_loja = sorted(pesos_loja)

    id_venda = Contador()
    id_item = Contador()
    id_mov = Contador()
    id_compra = Contador()
    id_compra_item = Contador()

    # ---------------- inventario inicial ----------------
    dt_inv = datetime.combine(primeiro_dia, hora(0, 30))
    linhas_inv = [
        (id_mov.proximo(), cod_loja, cod_produto, dt_inv, "INV", "E",
         Decimal(rng.randrange(20, 150)), None, dt_inv)
        for cod_loja in cods_loja for cod_produto in codigos
    ]
    gab.inseriu("mov_estoque", copiar(cur, "mov_estoque", linhas_inv))
    del linhas_inv

    # ---------------- historico dia a dia ----------------
    alvo = _distribuir_itens(escala, dias, pesos_loja)
    amostra_vendas: list[tuple[int, int, datetime]] = []   # (id_venda, cod_loja, dt_venda)
    passo_amostra = max(1, escala.itens_alvo // 4000)
    cancelados = 0
    cupom = {cod: 0 for cod in cods_loja}

    for dia in dias:
        vendas: list[tuple] = []
        itens: list[tuple] = []
        vnd: dict[tuple[int, int], Decimal] = {}
        dev: dict[tuple[int, int], Decimal] = {}

        for cod_loja in cods_loja:
            restantes = alvo[(dia, cod_loja)]
            while restantes > 0:
                k = min(restantes, rng.choices(ITENS_POR_CUPOM,
                                               cum_weights=CUM_ITENS_POR_CUPOM,
                                               k=1)[0])
                restantes -= k
                vid = id_venda.proximo()
                dt_venda = _hora_do_movimento(dia, rng)
                cancelada = rng.random() < PROB_CANCELAMENTO
                if cancelada:
                    cancelados += 1
                status = "C" if cancelada else "F"
                # O cancelamento acontece depois da venda: so o cabecalho muda.
                dt_alt_venda = dt_venda + timedelta(minutes=rng.randrange(5, 240)) \
                    if cancelada else dt_venda

                bruto = Decimal("0.00")
                desconto = Decimal("0.00")
                escolhidos = rng.choices(codigos, cum_weights=cum_pesos, k=k)
                for nr, cod_produto in enumerate(escolhidos, start=1):
                    prod = por_codigo[cod_produto]
                    qtd = sortear_qtd(prod.unid_medida, rng)
                    unit = prod.preco_venda
                    if rng.random() < PROB_PROMOCAO:
                        unit = _cent(unit * Decimal("0.90"))
                    valor = _cent(qtd * unit)
                    desc_item = _cent(valor * Decimal("0.10")) \
                        if rng.random() < PROB_DESCONTO_ITEM else Decimal("0.00")
                    bruto += valor
                    desconto += desc_item
                    itens.append((
                        id_item.proximo(), vid, nr, cod_produto, qtd, unit,
                        desc_item, valor - desc_item, prod.custo_medio,
                        "N", dt_venda,          # item NAO e tocado no cancelamento
                    ))
                    chave = (cod_loja, cod_produto)
                    destino = dev if cancelada else vnd
                    destino[chave] = destino.get(chave, Decimal(0)) + qtd

                vendas.append((
                    vid, cod_loja, rng.randrange(1, 13), _proximo_cupom(cupom, cod_loja),
                    dt_venda, status, bruto, desconto, bruto - desconto,
                    rng.choices(FORMAS_PAGTO, cum_weights=CUM_FORMA_PAGTO, k=1)[0],
                    dt_alt_venda,
                ))
                if vid % passo_amostra == 0 and not cancelada:
                    amostra_vendas.append((vid, cod_loja, dt_venda))

        gab.inseriu("mov_venda", copiar(cur, "mov_venda", vendas))
        gab.inseriu("mov_venda_item", copiar(cur, "mov_venda_item", itens))

        # Movimentos de estoque do dia: VND/DEV consolidados + compras + perdas.
        movimentos = _movimentos_do_dia(dia, vnd, dev, id_mov, codigos, cods_loja, rng)
        compras, compra_itens, mov_cmp = _compras_do_dia(
            dia, cods_loja, codigos, por_codigo, id_compra, id_compra_item,
            id_mov, rng)
        movimentos.extend(mov_cmp)

        gab.inseriu("mov_estoque", copiar(cur, "mov_estoque", movimentos))
        gab.inseriu("mov_compra", copiar(cur, "mov_compra", compras))
        gab.inseriu("mov_compra_item", copiar(cur, "mov_compra_item", compra_itens))

    gab.injetou("venda_cancelada_cabecalho", [])
    gab.injecoes["venda_cancelada_cabecalho"]["qtd"] = cancelados
    gab.injecoes["venda_cancelada_cabecalho"]["ids"] = []   # volume alto: so a contagem

    # ---------------- defeitos de qualidade ----------------
    _injetar_defeitos(cur, gab, DEFEITOS_INICIAL, amostra_vendas, por_codigo,
                      codigos, cods_loja, id_venda, id_item, rng,
                      dt_alt_novo=None)

    _ajustar_sequencias(cur)
    conn.commit()
    cur.execute("ANALYZE")
    conn.commit()


def _proximo_cupom(cupom: dict[int, int], cod_loja: int) -> int:
    cupom[cod_loja] += 1
    return cupom[cod_loja]


def _movimentos_do_dia(dia: date, vnd: dict, dev: dict, id_mov: Contador,
                       codigos: list[int], cods_loja: list[int],
                       rng: Random) -> list[tuple]:
    """VND e DEV consolidados por dia, mais alguns AJT e PRD."""
    dt_fecha = datetime.combine(dia, hora(23, 50))
    linhas = [
        (id_mov.proximo(), cod_loja, cod_produto, dt_fecha, "VND", "S",
         qtd, None, dt_fecha)
        for (cod_loja, cod_produto), qtd in vnd.items()
    ]
    linhas += [
        (id_mov.proximo(), cod_loja, cod_produto, dt_fecha, "DEV", "E",
         qtd, None, dt_fecha)
        for (cod_loja, cod_produto), qtd in dev.items()
    ]
    for cod_loja in cods_loja:
        for _ in range(rng.randrange(3, 9)):            # perdas
            dt = _hora_do_movimento(dia, rng)
            linhas.append((id_mov.proximo(), cod_loja, rng.choice(codigos), dt,
                           "PRD", "S", Decimal(rng.randrange(1, 12)), None, dt))
        for _ in range(rng.randrange(1, 5)):            # ajustes de inventario
            dt = _hora_do_movimento(dia, rng)
            sentido = rng.choice("ES")
            linhas.append((id_mov.proximo(), cod_loja, rng.choice(codigos), dt,
                           "AJT", sentido, Decimal(rng.randrange(1, 25)), None, dt))
    return linhas


def _compras_do_dia(dia: date, cods_loja: list[int], codigos: list[int],
                    por_codigo: dict[int, Produto], id_compra: Contador,
                    id_compra_item: Contador, id_mov: Contador,
                    rng: Random) -> tuple[list[tuple], list[tuple], list[tuple]]:
    """Uma nota de entrada por loja por dia, com os movimentos CMP correspondentes."""
    compras: list[tuple] = []
    itens: list[tuple] = []
    movimentos: list[tuple] = []
    for cod_loja in cods_loja:
        cid = id_compra.proximo()
        dt_entrada = datetime.combine(dia, hora(rng.randrange(5, 11), rng.randrange(60)))
        escolhidos = rng.sample(codigos, k=min(len(codigos), rng.randrange(40, 81)))
        total = Decimal("0.00")
        for cod_produto in escolhidos:
            prod = por_codigo[cod_produto]
            qtd = Decimal(rng.randrange(20, 121))
            custo = prod.custo_medio
            valor = _cent(qtd * custo)
            total += valor
            itens.append((id_compra_item.proximo(), cid, cod_produto, qtd,
                          custo, valor, dt_entrada))
            movimentos.append((id_mov.proximo(), cod_loja, cod_produto, dt_entrada,
                               "CMP", "E", qtd, str(cid), dt_entrada))
        compras.append((cid, cod_loja, rng.randrange(1, 31),
                        f"NF{cid:08d}", dia, dt_entrada, "R", total, dt_entrada))
    return compras, itens, movimentos


# ---------------------------------------------------------------------------
# Defeitos de qualidade (usados pela carga inicial e pelo delta)
# ---------------------------------------------------------------------------


def _item_defeituoso(id_item: Contador, vid: int, cod_produto: int,
                     prod: Produto | None, qtd, unit: Decimal, total,
                     dt_alt: datetime) -> tuple:
    custo = prod.custo_medio if prod else Decimal("1.0000")
    return (id_item.proximo(), vid, 1, cod_produto, qtd, unit,
            Decimal("0.00"), total, custo, "N", dt_alt)


def _injetar_defeitos(cur: psycopg.Cursor, gab: Gabarito, plano: dict[str, int],
                      amostra_vendas: list[tuple[int, int, datetime]],
                      por_codigo: dict[int, Produto], codigos: list[int],
                      cods_loja: list[int], id_venda: Contador,
                      id_item: Contador, rng: Random,
                      dt_alt_novo: datetime | None) -> None:
    """Escreve as linhas ruins em quantidade EXATA e registra os ids no gabarito.

    dt_alt_novo = None  -> a linha nasce com dt_alt historico (carga inicial).
    dt_alt_novo = agora -> a linha nasce "de hoje" (delta).
    """
    if not amostra_vendas:
        raise RuntimeError("sem vendas na origem para ancorar os defeitos")

    itens: list[tuple] = []
    vendas: list[tuple] = []

    def alvo() -> tuple[int, int, datetime]:
        return amostra_vendas[rng.randrange(len(amostra_vendas))]

    def quando(dt_venda: datetime) -> datetime:
        return dt_alt_novo if dt_alt_novo is not None else dt_venda

    # --- itens orfaos: apontam para venda inexistente ---
    ids = []
    for i in range(plano["item_orfao_venda"]):
        _vid, _loja, dt_venda = alvo()
        prod = por_codigo[rng.choice(codigos)]
        qtd = Decimal("1.000")
        itens.append(_item_defeituoso(id_item, BASE_VENDA_INEXISTENTE + i,
                                      prod.cod_produto, prod, qtd,
                                      prod.preco_venda, prod.preco_venda,
                                      quando(dt_venda)))
        ids.append(id_item.valor)
    gab.injetou("item_orfao_venda", ids)

    # --- itens orfaos: apontam para produto inexistente ---
    ids = []
    for i in range(plano["item_orfao_produto"]):
        vid, _loja, dt_venda = alvo()
        itens.append(_item_defeituoso(id_item, vid, BASE_PRODUTO_INEXISTENTE + i,
                                      None, Decimal("1.000"), Decimal("10.00"),
                                      Decimal("10.00"), quando(dt_venda)))
        ids.append(id_item.valor)
    gab.injetou("item_orfao_produto", ids)

    # --- quantidades invalidas ---
    # vl_total fica COERENTE com a qtd ruim de proposito: cada linha injetada
    # tem que falhar por UM motivo so, senao a conferencia da etapa 4 conta a
    # mesma linha em dois tipos de rejeicao.
    for tipo, qtd in (("item_qtd_nula", None),
                      ("item_qtd_zero", Decimal("0.000")),
                      ("item_qtd_negativa", Decimal("-2.000"))):
        ids = []
        for _ in range(plano[tipo]):
            vid, _loja, dt_venda = alvo()
            prod = por_codigo[rng.choice(codigos)]
            total = prod.preco_venda if qtd is None else _cent(qtd * prod.preco_venda)
            itens.append(_item_defeituoso(id_item, vid, prod.cod_produto, prod,
                                          qtd, prod.preco_venda,
                                          total, quando(dt_venda)))
            ids.append(id_item.valor)
        gab.injetou(tipo, ids)

    # --- vl_total divergente de qtd*unit - desconto ---
    ids = []
    for _ in range(plano["item_total_divergente"]):
        vid, _loja, dt_venda = alvo()
        prod = por_codigo[rng.choice(codigos)]
        q = Decimal(rng.randrange(1, 5))
        correto = _cent(q * prod.preco_venda)
        itens.append(_item_defeituoso(id_item, vid, prod.cod_produto, prod, q,
                                      prod.preco_venda,
                                      correto + DIVERGENCIA_VL_TOTAL,
                                      quando(dt_venda)))
        ids.append(id_item.valor)
    gab.injetou("item_total_divergente", ids)

    # --- cabecalhos invalidos: status fora do dominio e loja nula ---
    for tipo, status, loja_nula in (("venda_status_invalido", "X", False),
                                    ("venda_loja_nula", "F", True)):
        ids = []
        for _ in range(plano[tipo]):
            _vid, cod_loja, dt_venda = alvo()
            vid = id_venda.proximo()
            prod = por_codigo[rng.choice(codigos)]
            q = Decimal("1.000")
            valor = _cent(q * prod.preco_venda)
            dt_alt = quando(dt_venda)
            vendas.append((vid, None if loja_nula else cod_loja,
                           1, 999_000 + vid % 1000, dt_venda, status,
                           valor, Decimal("0.00"), valor, "PIX", dt_alt))
            itens.append(_item_defeituoso(id_item, vid, prod.cod_produto, prod,
                                          q, prod.preco_venda, valor, dt_alt))
            ids.append(vid)
        gab.injetou(tipo, ids)

    gab.inseriu("mov_venda", copiar(cur, "mov_venda", vendas))
    gab.inseriu("mov_venda_item", copiar(cur, "mov_venda_item", itens))


def _ajustar_sequencias(cur: psycopg.Cursor) -> None:
    """setval depois do COPY com ids explicitos, senao o proximo INSERT colide."""
    for tabela, coluna in SEQUENCIAS:
        cur.execute(
            f"SELECT setval(pg_get_serial_sequence('erp.{tabela}', '{coluna}'), "
            f"coalesce(max({coluna}), 1), max({coluna}) IS NOT NULL) FROM erp.{tabela}"
        )


# ---------------------------------------------------------------------------
# Delta: um dia de operacao
# ---------------------------------------------------------------------------


def carga_delta(conn: psycopg.Connection, semente: int, data_ref: date,
                vendas_por_loja: int | None, gab: Gabarito) -> None:
    rng = Random(semente)
    cur = conn.cursor()
    cur.execute("SET search_path = erp, public")
    agora = datetime.now().replace(microsecond=0)

    # ---------------- contexto ----------------
    cur.execute("SELECT cod_loja FROM erp.cad_loja WHERE ativa ORDER BY cod_loja")
    cods_loja = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT cod_produto, unid_medida, preco_venda, custo_medio "
                "FROM erp.cad_produto WHERE ativo ORDER BY cod_produto")
    produtos = [Produto(r[0], "", "", "", "", r[1], 0, r[2], r[3], True)
                for r in cur.fetchall()]
    if not cods_loja or not produtos:
        raise RuntimeError("origem vazia: rode a carga inicial antes do --delta")
    por_codigo = {p.cod_produto: p for p in produtos}
    codigos = [p.cod_produto for p in produtos]
    cum_pesos = _pesos_popularidade(produtos, Random(semente))

    id_venda = Contador(_proximo_id(cur, "mov_venda", "id_venda"))
    id_item = Contador(_proximo_id(cur, "mov_venda_item", "id_item"))
    id_mov = Contador(_proximo_id(cur, "mov_estoque", "id_mov"))

    if vendas_por_loja is None:
        cur.execute(
            "SELECT count(*) FROM erp.mov_venda WHERE dt_venda >= %s AND dt_venda < %s",
            (data_ref - timedelta(days=7), data_ref))
        recentes = cur.fetchone()[0]
        vendas_por_loja = max(10, round(recentes / (7 * len(cods_loja))))

    cur.execute("SELECT coalesce(max(nr_cupom), 0) FROM erp.mov_venda")
    base_cupom = cur.fetchone()[0]
    cupom = {cod: base_cupom for cod in cods_loja}

    # ---------------- 1. vendas novas de hoje ----------------
    vendas, itens, vnd, dev, novas = _lote_de_vendas(
        cods_loja, vendas_por_loja, data_ref, por_codigo, codigos, cum_pesos,
        id_venda, id_item, cupom, rng, dt_alt=agora)
    gab.inseriu("mov_venda", copiar(cur, "mov_venda", vendas))
    gab.inseriu("mov_venda_item", copiar(cur, "mov_venda_item", itens))
    gab.injetou("venda_nova_do_dia", novas)

    movimentos = _movimentos_do_dia(data_ref, vnd, dev, id_mov, codigos,
                                    cods_loja, rng)
    gab.inseriu("mov_estoque", copiar(cur, "mov_estoque", movimentos))

    # Toda venda que ja virou um caso especifico sai dos sorteios seguintes:
    # cada linha injetada tem que exercitar UM caso so, senao a conferencia da
    # etapa 4 conta a mesma venda em dois lugares.
    reservadas: list[int] = []

    # ---------------- 2. chegadas tardias (dt_alt retroativo) ----------------
    for tipo, quantos, faixa in (
        ("chegada_tardia_dentro_janela", DELTA_TARDIA_DENTRO_JANELA, (1, 5)),
        ("chegada_tardia_fora_janela", DELTA_TARDIA_FORA_JANELA,
         (DELTA_DIAS_FORA_JANELA, DELTA_DIAS_FORA_JANELA)),
    ):
        vendas, itens, ids = [], [], []
        for _ in range(quantos):
            atraso = rng.randint(*faixa)
            dia = data_ref - timedelta(days=atraso)
            v, i, _vnd, _dev, novos = _lote_de_vendas(
                [rng.choice(cods_loja)], 1, dia, por_codigo, codigos, cum_pesos,
                id_venda, id_item, cupom, rng, dt_alt=None)
            vendas.extend(v)
            itens.extend(i)
            ids.extend(novos)
        gab.inseriu("mov_venda", copiar(cur, "mov_venda", vendas))
        gab.inseriu("mov_venda_item", copiar(cur, "mov_venda_item", itens))
        gab.injetou(tipo, ids)
        reservadas.extend(ids)

    # ---------------- 3. alteracoes em vendas de 1 a 5 dias atras ----------------
    # As chegadas tardias do passo 2 tambem tem dt_venda de 1 a 5 dias atras.
    # Se caissem aqui, a trigger recarimbaria o dt_alt delas com a hora de
    # agora e elas deixariam de ser chegada tardia.
    cur.execute(
        "SELECT id_venda FROM erp.mov_venda "
        "WHERE dt_venda >= %s AND dt_venda < %s AND status = 'F' "
        "AND NOT (id_venda = ANY(%s)) "
        "ORDER BY id_venda",
        (data_ref - timedelta(days=5), data_ref, reservadas))
    candidatos = [r[0] for r in cur.fetchall()]
    rng.shuffle(candidatos)
    precisa = DELTA_ATUALIZACAO_TARDIA + DELTA_CANCELAMENTO
    if len(candidatos) < precisa:
        raise RuntimeError(
            f"so {len(candidatos)} vendas nos ultimos 5 dias; preciso de {precisa}. "
            "Rode a carga inicial com uma escala maior."
        )
    alvo_update = candidatos[:DELTA_ATUALIZACAO_TARDIA]
    alvo_cancel = candidatos[DELTA_ATUALIZACAO_TARDIA:precisa]

    # UPDATE sem informar dt_alt => a trigger carimba agora (dt_venda continua antigo).
    cur.execute(
        "UPDATE erp.mov_venda SET forma_pagto = 'VOUCHER' WHERE id_venda = ANY(%s)",
        (alvo_update,))
    gab.atualizou("mov_venda", cur.rowcount)
    gab.injetou("atualizacao_tardia", alvo_update)

    # Cancelamento SO no cabecalho: os itens continuam 'N' com dt_alt antigo.
    cur.execute(
        "UPDATE erp.mov_venda SET status = 'C' WHERE id_venda = ANY(%s)",
        (alvo_cancel,))
    gab.atualizou("mov_venda", cur.rowcount)
    gab.injetou("venda_cancelada_cabecalho", alvo_cancel)
    reservadas.extend(alvo_update)
    reservadas.extend(alvo_cancel)

    # ---------------- 4. linhas inconsistentes ----------------
    # Mesma razao: um item ruim pendurado numa venda cancelada daria a ela um
    # item com dt_alt de hoje, desmontando justamente a armadilha do passo 3.
    cur.execute(
        "SELECT id_venda, cod_loja, dt_venda FROM erp.mov_venda "
        "WHERE dt_venda >= %s AND cod_loja IS NOT NULL AND NOT (id_venda = ANY(%s)) "
        "ORDER BY id_venda LIMIT 500",
        (data_ref - timedelta(days=2), reservadas))
    amostra = [(r[0], r[1], r[2]) for r in cur.fetchall()]
    _injetar_defeitos(cur, gab, DEFEITOS_DELTA, amostra, por_codigo, codigos,
                      cods_loja, id_venda, id_item, rng, dt_alt_novo=agora)

    _ajustar_sequencias(cur)
    conn.commit()


def _proximo_id(cur: psycopg.Cursor, tabela: str, coluna: str) -> int:
    cur.execute(f"SELECT coalesce(max({coluna}), 0) + 1 FROM erp.{tabela}")
    return cur.fetchone()[0]


def _lote_de_vendas(cods_loja: list[int], por_loja: int, dia: date,
                    por_codigo: dict[int, Produto], codigos: list[int],
                    cum_pesos: list[float], id_venda: Contador, id_item: Contador,
                    cupom: dict[int, int], rng: Random,
                    dt_alt: datetime | None):
    """Monta cupons de um dia.

    dt_alt = None  -> a linha nasce com dt_alt igual a dt_venda (chegada tardia:
                      fica ABAIXO da marca-d'agua do incremental).
    dt_alt = agora -> venda normal do dia.
    """
    vendas: list[tuple] = []
    itens: list[tuple] = []
    vnd: dict[tuple[int, int], Decimal] = {}
    dev: dict[tuple[int, int], Decimal] = {}
    ids: list[int] = []

    for cod_loja in cods_loja:
        for _ in range(por_loja):
            vid = id_venda.proximo()
            ids.append(vid)
            dt_venda = _hora_do_movimento(dia, rng)
            carimbo = dt_alt if dt_alt is not None else dt_venda
            k = rng.choices(ITENS_POR_CUPOM, cum_weights=CUM_ITENS_POR_CUPOM, k=1)[0]
            bruto = Decimal("0.00")
            desconto = Decimal("0.00")
            for nr, cod_produto in enumerate(rng.choices(codigos, cum_weights=cum_pesos, k=k),
                                             start=1):
                prod = por_codigo[cod_produto]
                qtd = sortear_qtd(prod.unid_medida, rng)
                unit = prod.preco_venda
                if rng.random() < PROB_PROMOCAO:
                    unit = _cent(unit * Decimal("0.90"))
                valor = _cent(qtd * unit)
                desc_item = _cent(valor * Decimal("0.10")) \
                    if rng.random() < PROB_DESCONTO_ITEM else Decimal("0.00")
                bruto += valor
                desconto += desc_item
                itens.append((id_item.proximo(), vid, nr, cod_produto, qtd, unit,
                              desc_item, valor - desc_item, prod.custo_medio,
                              "N", carimbo))
                chave = (cod_loja, cod_produto)
                vnd[chave] = vnd.get(chave, Decimal(0)) + qtd
            cupom[cod_loja] += 1
            vendas.append((vid, cod_loja, rng.randrange(1, 13), cupom[cod_loja],
                           dt_venda, "F", bruto, desconto, bruto - desconto,
                           rng.choices(FORMAS_PAGTO, cum_weights=CUM_FORMA_PAGTO, k=1)[0],
                           carimbo))
    return vendas, itens, vnd, dev, ids


# ---------------------------------------------------------------------------
# Gabarito
# ---------------------------------------------------------------------------


def contar_origem(conn: psycopg.Connection) -> dict[str, int]:
    cur = conn.cursor()
    saida = {}
    for tabela in TABELAS_ERP:
        cur.execute(f"SELECT count(*) FROM erp.{tabela}")
        saida[tabela] = cur.fetchone()[0]
    return saida


def escrever_gabarito(caminho: Path, dados: dict) -> None:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    caminho.write_text(json.dumps(dados, indent=2, ensure_ascii=False,
                                  sort_keys=False) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def construir_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m pipeline.gerador",
        description="Gera dados sinteticos no banco erp_origem (schema erp).")
    p.add_argument("--escala", choices=sorted(ESCALAS),
                   help="carga inicial: apaga a origem e recria cadastros + historico")
    p.add_argument("--delta", action="store_true",
                   help="simula um dia de operacao sobre a base existente")
    p.add_argument("--semente", type=int, default=42,
                   help="semente do gerador (padrao: 42)")
    p.add_argument("--data-ref", type=date.fromisoformat, default=date.today(),
                   help="data de referencia AAAA-MM-DD (padrao: hoje). O historico "
                        "vai ate data-ref - 1; o --delta escreve em data-ref.")
    p.add_argument("--vendas-por-loja", type=int, default=None,
                   help="--delta: cupons novos por loja (padrao: media dos 7 dias)")
    p.add_argument("--saida", type=Path, default=RAIZ / "saida" / "gerador_resumo.json",
                   help="arquivo do gabarito (padrao: saida/gerador_resumo.json)")
    return p


def main(argv: list[str] | None = None) -> int:
    args = construir_parser().parse_args(argv)
    if bool(args.escala) == bool(args.delta):
        print("erro: informe --escala OU --delta (um dos dois).", file=sys.stderr)
        return 2

    gab = Gabarito()
    inicio = time.perf_counter()
    with conectar_origem_app() as conn:
        if args.delta:
            carga_delta(conn, args.semente, args.data_ref,
                        args.vendas_por_loja, gab)
            escala = None
        else:
            escala = ESCALAS[args.escala]
            carga_inicial(conn, escala, args.semente, args.data_ref, gab)
        duracao = round(time.perf_counter() - inicio, 2)
        totais = contar_origem(conn)

    resumo = {
        "versao_gabarito": 1,
        "modo": "delta" if args.delta else "inicial",
        "escala": escala.nome if escala else None,
        "semente": args.semente,
        "data_ref": args.data_ref.isoformat(),
        "gerado_em": datetime.now().astimezone().isoformat(timespec="seconds"),
        "duracao_s": duracao,
        "janela_reprocessamento_dias": JANELA_REPROCESSAMENTO_DIAS,
        "parametros": asdict(escala) if escala else {},
        "inseridos": gab.inseridos,
        "atualizados": gab.atualizados,
        "injecoes": gab.injecoes,
        "total_origem": totais,
    }
    escrever_gabarito(args.saida, resumo)

    print(f"modo={resumo['modo']} escala={resumo['escala']} "
          f"semente={args.semente} data_ref={args.data_ref} em {duracao}s")
    for tabela, n in totais.items():
        print(f"  {tabela:<16} {n:>10,}".replace(",", "."))
    print(f"gabarito: {args.saida}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
