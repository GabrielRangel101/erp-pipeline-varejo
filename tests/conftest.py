"""
Apoio comum aos testes.

ATENCAO: os testes marcados com @pytest.mark.banco RECRIAM o conteudo do banco
erp_origem (TRUNCATE + carga em escala de teste). Isso e aceitavel porque o
dado e 100% sintetico e volta com um comando:

    python -m pipeline.gerador --escala pequena --semente 42

Se um dia a origem guardar algo que nao da para refazer, os testes passam a
precisar de um banco proprio (erp_origem_teste) em vez de reaproveitar este.
"""

from __future__ import annotations

from datetime import date

import pytest

from pipeline.gerador import Escala, Gabarito as Gabarito_t

# Escala minima: rapida, mas grande o bastante para o --delta achar as 70
# vendas que ele precisa alterar nos ultimos 5 dias.
ESCALA_TESTE = Escala("teste", dias=12, itens_alvo=4_000, lojas=4,
                      produtos=200, fornecedores=10)

SEMENTE = 2026
DATA_REF = date(2026, 6, 15)


@pytest.fixture(scope="session")
def conexao():
    """Conexao com erp_origem como erp_app. Pula o teste se o Postgres nao subiu.

    AUTOCOMMIT de proposito. Sem isso, um simples SELECT deixa a conexao em
    "idle in transaction" segurando ACCESS SHARE, e o TRUNCATE da proxima
    geracao de dados espera esse lock para sempre. Quem precisa desfazer uma
    escrita usa "with conexao.transaction():" e levanta uma excecao.
    """
    from pipeline.db import conectar_origem_app
    try:
        conn = conectar_origem_app(autocommit=True)
    except Exception as erro:                      # noqa: BLE001
        pytest.skip(f"Postgres indisponivel ({erro}). Rode: docker compose up -d")
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def conexao_dw():
    """Conexao com dw_varejo como pipeline. Autocommit pelo mesmo motivo acima."""
    from pipeline.db import conectar_dw
    try:
        conn = conectar_dw(autocommit=True)
    except Exception as erro:                      # noqa: BLE001
        pytest.skip(f"Postgres indisponivel ({erro}). Rode: docker compose up -d")
    yield conn
    conn.close()


def gerar_origem(semente: int = SEMENTE) -> None:
    """Recria a origem na escala de teste. Usado pelos testes de carga."""
    from pipeline.db import conectar_origem_app
    from pipeline.gerador import Gabarito, carga_inicial
    with conectar_origem_app() as conn:
        carga_inicial(conn, ESCALA_TESTE, semente, DATA_REF, Gabarito())


def gerar_delta(semente: int = SEMENTE + 1) -> Gabarito_t:
    """Um dia de operacao sobre a origem ja gerada. Devolve o gabarito."""
    from pipeline.db import conectar_origem_app
    from pipeline.gerador import Gabarito, carga_delta
    gabarito = Gabarito()
    with conectar_origem_app() as conn:
        carga_delta(conn, semente, DATA_REF, None, gabarito)
    return gabarito


def contar(conexao, sql: str, parametros: tuple = ()) -> int:
    cur = conexao.cursor()
    cur.execute(sql, parametros)
    return cur.fetchone()[0]
