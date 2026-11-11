"""
Cadastros sinteticos do ERP A: lojas, fornecedores e produtos.

Tudo ficticio de proposito (ver CLAUDE.md): nenhuma empresa real, nenhum dado
pessoal, nenhum CNPJ. Os codigos de barras usam o prefixo GS1 200-299,
reservado para uso interno de loja, que por definicao nao colide com produto
de fabricante.

Tudo aqui e deterministico: mesma semente, mesmos cadastros.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from random import Random

CENTAVO = Decimal("0.01")
MILESIMO = Decimal("0.001")

# ---------------------------------------------------------------------------
# Lojas
# ---------------------------------------------------------------------------

# (cod_loja, nome, cidade, uf, formato, dt_abertura, peso de movimento)
LOJAS_BASE: list[tuple[int, str, str, str, str, date, float]] = [
    (1, "LOJA CENTRO", "Campinas", "SP", "HIPER", date(2016, 3, 14), 1.60),
    (2, "LOJA NORTE", "Sorocaba", "SP", "SUPER", date(2018, 9, 1), 1.00),
    (3, "LOJA SUL", "Santos", "SP", "SUPER", date(2019, 5, 20), 0.90),
    (4, "LOJA LITORAL", "Jundiai", "SP", "EXPRESS", date(2021, 11, 8), 0.45),
]

# ---------------------------------------------------------------------------
# Fornecedores
# ---------------------------------------------------------------------------

NOMES_FANTASIA = [
    "ALFA", "BETA", "GAMA", "DELTA", "EPSILON", "ZETA", "ETA", "TETA",
    "IOTA", "CAPA", "LAMBDA", "SIGMA", "TAU", "UPSILON", "OMEGA", "ORION",
    "VERTICE", "PRIMA", "NOVA", "ZENITE", "APICE", "BOREAL", "MERIDIANO",
    "POLARIS", "VEGA", "ATLAS", "CERES", "DORADO", "FENIX", "GRIFO",
]

RAZOES = ["DISTRIBUIDORA {} LTDA", "{} ATACADO SA", "{} ALIMENTOS LTDA",
          "COMERCIAL {} LTDA", "{} SUPRIMENTOS SA"]

UFS = ["SP", "MG", "PR", "RS", "SC", "RJ", "GO", "BA", "PE", "CE"]

# ---------------------------------------------------------------------------
# Produtos
# ---------------------------------------------------------------------------

# departamento -> (unid_medida, preco minimo, preco maximo, margem min, margem max)
DEPARTAMENTOS: dict[str, tuple[str, float, float, float, float]] = {
    "MERCEARIA":  ("UN",  3.0,  45.0, 0.18, 0.35),
    "HORTIFRUTI": ("KG",  2.5,  30.0, 0.25, 0.45),
    "ACOUGUE":    ("KG", 18.0,  95.0, 0.12, 0.28),
    "PADARIA":    ("UN",  2.0,  35.0, 0.30, 0.55),
    "FRIOS":      ("UN",  6.0,  70.0, 0.15, 0.32),
    "CONGELADOS": ("UN",  8.0,  60.0, 0.18, 0.33),
    "BEBIDAS":    ("UN",  2.5,  90.0, 0.14, 0.30),
    "LIMPEZA":    ("UN",  3.0,  55.0, 0.20, 0.38),
    "HIGIENE":    ("UN",  4.0,  75.0, 0.22, 0.42),
    "PET":        ("UN",  9.0, 180.0, 0.20, 0.40),
}

SECOES: dict[str, dict[str, list[str]]] = {
    "MERCEARIA": {
        "GRAOS E CEREAIS": ["ARROZ TIPO 1", "FEIJAO CARIOCA", "FEIJAO PRETO",
                            "LENTILHA", "GRAO DE BICO", "AVEIA EM FLOCOS"],
        "MASSAS": ["MACARRAO ESPAGUETE", "MACARRAO PARAFUSO",
                   "MACARRAO INSTANTANEO", "MASSA PARA LASANHA"],
        "OLEOS E MOLHOS": ["OLEO DE SOJA", "AZEITE DE OLIVA", "MOLHO DE TOMATE",
                           "VINAGRE DE ALCOOL", "MAIONESE"],
        "MATINAIS": ["ACUCAR REFINADO", "CAFE TORRADO MOIDO", "ADOCANTE LIQUIDO",
                     "CHA MATE", "ACHOCOLATADO EM PO"],
    },
    "HORTIFRUTI": {
        "FRUTAS": ["BANANA PRATA", "MACA GALA", "LARANJA PERA", "MAMAO FORMOSA",
                   "UVA ITALIA", "MELANCIA"],
        "LEGUMES": ["TOMATE", "CEBOLA", "BATATA", "CENOURA", "ABOBRINHA"],
        "VERDURAS": ["ALFACE", "COUVE", "RUCULA", "ESPINAFRE"],
    },
    "ACOUGUE": {
        "BOVINOS": ["PATINHO", "ALCATRA", "COXAO MOLE", "ACEM", "COSTELA"],
        "SUINOS": ["LOMBO SUINO", "PERNIL SUINO", "COSTELINHA"],
        "AVES": ["PEITO DE FRANGO", "COXA E SOBRECOXA", "FRANGO INTEIRO"],
    },
    "PADARIA": {
        "PAES": ["PAO FRANCES", "PAO DE FORMA", "PAO INTEGRAL", "BAGUETE"],
        "CONFEITARIA": ["BOLO DE CENOURA", "SONHO", "TORTA DE LIMAO"],
        "SALGADOS": ["COXINHA", "PAO DE QUEIJO", "ESFIHA"],
    },
    "FRIOS": {
        "LATICINIOS": ["QUEIJO MUSSARELA", "QUEIJO PRATO", "REQUEIJAO",
                       "IOGURTE NATURAL", "MANTEIGA"],
        "EMBUTIDOS": ["PRESUNTO COZIDO", "MORTADELA", "SALAME", "LINGUICA TOSCANA"],
    },
    "CONGELADOS": {
        "PRATOS PRONTOS": ["LASANHA CONGELADA", "PIZZA CONGELADA", "ESCONDIDINHO"],
        "SORVETES": ["SORVETE DE CREME", "SORVETE DE CHOCOLATE", "PICOLE"],
        "VEGETAIS CONGELADOS": ["BATATA PALITO", "ERVILHA CONGELADA", "BROCOLIS"],
    },
    "BEBIDAS": {
        "NAO ALCOOLICAS": ["REFRIGERANTE COLA", "SUCO DE UVA", "AGUA MINERAL",
                           "CHA GELADO", "ENERGETICO"],
        "ALCOOLICAS": ["CERVEJA PILSEN", "VINHO TINTO SECO", "ESPUMANTE BRUT"],
        "CAFE E CHA": ["CAFE EM CAPSULA", "CAPPUCCINO SOLUVEL"],
    },
    "LIMPEZA": {
        "ROUPAS": ["SABAO EM PO", "AMACIANTE", "SABAO LIQUIDO"],
        "CASA": ["DETERGENTE NEUTRO", "DESINFETANTE", "AGUA SANITARIA",
                 "LIMPADOR MULTIUSO", "ESPONJA DE LOUCA"],
    },
    "HIGIENE": {
        "BANHO": ["SABONETE EM BARRA", "SHAMPOO", "CONDICIONADOR", "GEL DE BANHO"],
        "ORAL": ["CREME DENTAL", "ENXAGUANTE BUCAL", "ESCOVA DENTAL"],
        "PAPEL": ["PAPEL HIGIENICO", "LENCO DE PAPEL", "TOALHA DE PAPEL"],
    },
    "PET": {
        "CAES": ["RACAO PARA CAES ADULTOS", "PETISCO CANINO", "TAPETE HIGIENICO"],
        "GATOS": ["RACAO PARA GATOS", "AREIA SANITARIA", "SACHE FELINO"],
    },
}

MARCAS = ["ALFA", "BETA", "GAMA", "DELTA", "ORION", "VERTICE", "PRIMA",
          "NOVA", "ZENITE", "APICE", "BOREAL", "MERIDIANO"]

# Embalagem por secao: arroz nao vem em litro e refrigerante nao vem em quilo.
TAM_PESO = ["200G", "350G", "500G", "1KG", "2KG", "5KG"]
TAM_VOLUME = ["300ML", "500ML", "900ML", "1L", "2L"]
TAM_PACOTE = ["2UN", "4UN", "6UN", "12UN"]

TAMANHO_POR_SECAO: dict[str, list[str]] = {
    "GRAOS E CEREAIS": TAM_PESO,
    "MASSAS": TAM_PESO,
    "OLEOS E MOLHOS": TAM_VOLUME,
    "MATINAIS": TAM_PESO,
    "PAES": TAM_PESO,
    "CONFEITARIA": ["350G", "500G", "1KG", "2KG"],
    "SALGADOS": TAM_PACOTE,
    "LATICINIOS": TAM_PESO,
    "EMBUTIDOS": TAM_PESO,
    "PRATOS PRONTOS": TAM_PESO,
    "SORVETES": TAM_VOLUME,
    "VEGETAIS CONGELADOS": TAM_PESO,
    "NAO ALCOOLICAS": TAM_VOLUME,
    "ALCOOLICAS": TAM_VOLUME,
    "CAFE E CHA": TAM_PESO,
    "ROUPAS": TAM_PESO,
    "CASA": TAM_VOLUME,
    "BANHO": TAM_VOLUME,
    "ORAL": TAM_PESO,
    "PAPEL": TAM_PACOTE,
    "CAES": TAM_PESO,
    "GATOS": TAM_PESO,
}

# Excecoes dentro da secao.
TAMANHO_POR_PRODUTO: dict[str, list[str]] = {
    "ADOCANTE LIQUIDO": TAM_VOLUME,
    "AMACIANTE": TAM_VOLUME,
    "SABAO LIQUIDO": TAM_VOLUME,
    "ENXAGUANTE BUCAL": TAM_VOLUME,
    "ESPONJA DE LOUCA": TAM_PACOTE,
    "ESCOVA DENTAL": TAM_PACOTE,
    "SABONETE EM BARRA": TAM_PACOTE,
    "TAPETE HIGIENICO": TAM_PACOTE,
    "PETISCO CANINO": TAM_PACOTE,
    "SACHE FELINO": TAM_PACOTE,
    "CAFE EM CAPSULA": TAM_PACOTE,
    "PAO FRANCES": TAM_PACOTE,
    "SONHO": TAM_PACOTE,
    "COXINHA": TAM_PACOTE,
    "PAO DE QUEIJO": TAM_PACOTE,
    "ESFIHA": TAM_PACOTE,
    "PICOLE": TAM_PACOTE,
    "AREIA SANITARIA": TAM_PESO,
}


@dataclass(frozen=True)
class Produto:
    cod_produto: int
    cod_barras: str
    descricao: str
    departamento: str
    secao: str
    unid_medida: str
    cod_fornecedor: int
    preco_venda: Decimal
    custo_medio: Decimal
    ativo: bool


def digito_ean13(corpo12: str) -> int:
    """Digito verificador EAN-13 (pesos 1 e 3 alternados da esquerda)."""
    soma = sum(int(d) * (3 if i % 2 else 1) for i, d in enumerate(corpo12))
    return (10 - soma % 10) % 10


def cod_barras_interno(sequencial: int, rng: Random) -> str:
    """EAN-13 com prefixo 200-299 (faixa GS1 de uso interno)."""
    corpo = f"2{rng.randint(0, 99):02d}{sequencial:09d}"
    return corpo + str(digito_ean13(corpo))


def _dinheiro(valor: float, casas: Decimal = CENTAVO) -> Decimal:
    return Decimal(str(valor)).quantize(casas, rounding=ROUND_HALF_UP)


def gerar_lojas(dt_alt) -> list[tuple]:
    """Linhas de erp.cad_loja na ordem das colunas de COLUNAS_LOJA."""
    return [
        (cod, nome, cidade, uf, formato, abertura, True, dt_alt)
        for cod, nome, cidade, uf, formato, abertura, _peso in LOJAS_BASE
    ]


def peso_das_lojas() -> dict[int, float]:
    return {cod: peso for cod, _n, _c, _u, _f, _a, peso in LOJAS_BASE}


def gerar_fornecedores(quantidade: int, rng: Random, dt_alt) -> list[tuple]:
    """Linhas de erp.cad_fornecedor."""
    linhas = []
    for i in range(quantidade):
        nome = NOMES_FANTASIA[i % len(NOMES_FANTASIA)]
        sufixo = "" if i < len(NOMES_FANTASIA) else f" {i // len(NOMES_FANTASIA) + 1}"
        razao = RAZOES[i % len(RAZOES)].format(nome + sufixo)
        linhas.append((
            i + 1,
            razao,
            rng.choice(UFS),
            rng.random() > 0.05,                      # ~5% inativos
            dt_alt.date(),
            dt_alt,
        ))
    return linhas


def _combinacoes_base() -> list[tuple[str, str, str]]:
    """(departamento, secao, nome base) em ordem estavel."""
    saida = []
    for departamento, secoes in SECOES.items():
        for secao, bases in secoes.items():
            for base in bases:
                saida.append((departamento, secao, base))
    return saida


def gerar_produtos(quantidade: int, qtd_fornecedores: int, rng: Random,
                   dt_alt) -> list[Produto]:
    """Produtos deterministicos, combinando base x marca x tamanho."""
    combinacoes = _combinacoes_base()
    produtos: list[Produto] = []
    for i in range(quantidade):
        departamento, secao, base = combinacoes[i % len(combinacoes)]
        unid, p_min, p_max, m_min, m_max = DEPARTAMENTOS[departamento]
        marca = MARCAS[(i // len(combinacoes)) % len(MARCAS)]
        if unid == "KG":
            descricao = f"{base} {marca}"
        else:
            tamanhos = TAMANHO_POR_PRODUTO.get(base, TAMANHO_POR_SECAO[secao])
            descricao = f"{base} {rng.choice(tamanhos)} {marca}"
        # Preco com cauda: a maioria barata, alguns itens caros.
        preco = p_min + (p_max - p_min) * (rng.random() ** 2.2)
        margem = rng.uniform(m_min, m_max)
        produtos.append(Produto(
            cod_produto=1000 + i + 1,
            cod_barras=cod_barras_interno(i + 1, rng),
            descricao=descricao[:120],
            departamento=departamento,
            secao=secao,
            unid_medida=unid,
            cod_fornecedor=rng.randint(1, qtd_fornecedores),
            preco_venda=_dinheiro(preco),
            custo_medio=_dinheiro(preco * (1 - margem), Decimal("0.0001")),
            ativo=rng.random() > 0.03,                 # ~3% inativos
        ))
    return produtos


def linhas_produto(produtos: list[Produto], dt_alt) -> list[tuple]:
    """Produtos no formato de COLUNAS_PRODUTO."""
    return [
        (p.cod_produto, p.cod_barras, p.descricao, p.departamento, p.secao,
         p.unid_medida, p.cod_fornecedor, p.preco_venda, p.custo_medio,
         p.ativo, dt_alt)
        for p in produtos
    ]
