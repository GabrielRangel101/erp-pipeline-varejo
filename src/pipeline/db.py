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


def conectar_origem_leitura(autocommit: bool = False) -> psycopg.Connection:
    """Origem como erp_ro (somente leitura). E o que o pipeline usa."""
    return conectar(ERP_RO, autocommit=autocommit)


def conectar_dw(autocommit: bool = False) -> psycopg.Connection:
    """DW como pipeline (dono dos schemas ctl/stg/trt/dw)."""
    return conectar(PIPELINE, autocommit=autocommit)
