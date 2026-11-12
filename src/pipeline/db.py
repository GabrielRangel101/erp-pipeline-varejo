"""
Conexoes com o Postgres.

Regra do projeto: credencial SO entra por variavel de ambiente. Nenhuma senha
fica no codigo nem no Git. O arquivo .env da raiz e lido por conveniencia de
desenvolvimento, mas NUNCA sobrescreve o que ja esta no ambiente (em CI as
variaveis vem dos secrets e nao existe .env).

Tres papeis, tres conexoes distintas, por principio do menor privilegio:
  erp_app   -> erp_origem  dono da origem; usado SO pelo gerador de dados
  erp_ro    -> erp_origem  somente leitura; usado pelo pipeline de extracao
  pipeline  -> dw_varejo   dono do DW; usado pelas cargas
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import psycopg
from psycopg import conninfo as pg_conninfo

RAIZ = Path(__file__).resolve().parents[2]

BANCO_ORIGEM = "erp_origem"
BANCO_DW = "dw_varejo"


def carregar_env(arquivo: Path | None = None) -> None:
    """Le o .env da raiz para o ambiente, sem sobrescrever variaveis ja definidas."""
    caminho = arquivo if arquivo is not None else RAIZ / ".env"
    if not caminho.exists():
        return
    for linha in caminho.read_text(encoding="utf-8").splitlines():
        linha = linha.strip()
        if not linha or linha.startswith("#") or "=" not in linha:
            continue
        chave, _, valor = linha.partition("=")
        os.environ.setdefault(chave.strip(), valor.strip())


@dataclass(frozen=True)
class Credencial:
    """Papel do banco + variavel de ambiente que guarda a senha dele."""

    usuario: str
    banco: str
    var_senha: str


ERP_APP = Credencial("erp_app", BANCO_ORIGEM, "ERP_APP_PASSWORD")
ERP_RO = Credencial("erp_ro", BANCO_ORIGEM, "ERP_RO_PASSWORD")
PIPELINE = Credencial("pipeline", BANCO_DW, "PIPELINE_PASSWORD")


def _conninfo(cred: Credencial) -> str:
    carregar_env()
    senha = os.environ.get(cred.var_senha)
    if not senha:
        raise RuntimeError(
            f"variavel {cred.var_senha} nao definida. "
            "Copie .env.example para .env e preencha as senhas, "
            "ou exporte a variavel no ambiente."
        )
    return pg_conninfo.make_conninfo(
        host=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "5432"),
        dbname=cred.banco,
        user=cred.usuario,
        password=senha,
        application_name=f"erp_pipeline/{cred.usuario}",
        connect_timeout=10,
    )


def conectar(cred: Credencial, autocommit: bool = False) -> psycopg.Connection:
    """Abre conexao para o papel informado."""
    return psycopg.connect(_conninfo(cred), autocommit=autocommit)


def conectar_origem_app(autocommit: bool = False) -> psycopg.Connection:
    """Origem como erp_app (escrita). So o gerador de dados usa isto."""
    return conectar(ERP_APP, autocommit=autocommit)


def conectar_origem_leitura(autocommit: bool = False,
                            snapshot_consistente: bool = False
                            ) -> psycopg.Connection:
    """Origem como erp_ro (somente leitura). E o que o pipeline usa.

    snapshot_consistente abre a conexao em REPEATABLE READ + READ ONLY. A
    partir do primeiro comando ela passa a ver um RETRATO FIXO da origem, que
    nao muda ate o fim da transacao.

    POR QUE ISSO IMPORTA
    A extracao le a origem e, minutos depois, a conferencia le a origem de
    novo para somar o lado de ca da conta. O ERP nao para enquanto o pipeline
    roda: uma venda gravada entre as duas leituras aparece na contagem da
    conferencia sem estar no destino, e a carga reprova sem ter perdido nada.
    Falso positivo em portao de qualidade e caro duas vezes - derruba a carga
    e ensina o operador a desconfiar do alarme.

    Com o retrato fixo, os dois lados julgam o MESMO instante da origem. O que
    o ERP gravar durante a carga entra na proxima, pela marca-d'agua.

    PRECO: o snapshot segura o xmin da origem enquanto a transacao estiver
    aberta, ou seja, durante a carga inteira. O autovacuum do ERP nao recolhe
    versao de linha mais nova que isso nesse periodo.

    So vale com autocommit desligado: em autocommit cada comando e a sua
    propria transacao e nao ha snapshot para compartilhar.
    """
    conn = conectar(ERP_RO, autocommit=autocommit)
    if snapshot_consistente:
        # Tem que ser antes do primeiro comando: depois disso a transacao ja
        # comecou e o Postgres recusa a troca.
        conn.read_only = True
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
    return conn


def conectar_dw(autocommit: bool = False) -> psycopg.Connection:
    """DW como pipeline (dono dos schemas ctl/stg/trt/dw)."""
    return conectar(PIPELINE, autocommit=autocommit)
