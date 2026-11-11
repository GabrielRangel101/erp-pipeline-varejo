"""
Leitura e validacao do conector YAML, e montagem do SELECT de extracao.

Este modulo e o unico lugar que sabe como o ERP de origem se chama. Todo o
resto do pipeline fala em nomes canonicos (loja, venda_item, ...). Integrar um
ERP novo e escrever config/conectores/<erp>.yaml - nenhum .py muda.

Seguranca: nome de tabela e de coluna vindos do YAML sao validados contra
[a-z_][a-z0-9_]* e compostos com psycopg.sql.Identifier, nunca por f-string.
Valores (sistema_origem, id_execucao, data de corte) viajam como parametro.
A unica porta aberta e a forma {expr: "..."}, que e SQL cru de proposito; o
arquivo de conector tem o mesmo nivel de confianca de um .py do repositorio.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import yaml
from psycopg import sql

from pipeline.db import RAIZ

PASTA_CONECTORES = RAIZ / "config" / "conectores"

# Postgres dobra identificador sem aspas para minusculo; exigir minusculo aqui
# evita a surpresa de um YAML com "Cod_Loja" que nao acha a coluna.
NOME_VALIDO = re.compile(r"^[a-z_][a-z0-9_]*$")

CHAVES_ENTIDADE = {"tabela", "chave", "coluna_alteracao", "colunas",
                   "reextrair_quando_pai_mudar"}
CHAVES_PAI = {"tabela_pai", "coluna_ligacao", "coluna_ligacao_pai",
              "coluna_alteracao_pai"}


class ConectorInvalido(ValueError):
    """O YAML do conector esta malformado. A mensagem diz onde."""


def _nome(valor, onde: str) -> str:
    if not isinstance(valor, str) or not NOME_VALIDO.match(valor):
        raise ConectorInvalido(
            f"{onde}: {valor!r} nao e um identificador valido "
            f"(esperado minusculo, no formato {NOME_VALIDO.pattern})")
    return valor


@dataclass(frozen=True)
class Pai:
    """Como achar os filhos cujo PAI mudou (cancelamento so no cabecalho)."""

    tabela: str
    coluna_ligacao: str          # coluna na tabela filha
    coluna_ligacao_pai: str      # coluna correspondente na tabela pai
    coluna_alteracao: str        # coluna de ultima alteracao do pai


@dataclass(frozen=True)
class Entidade:
    nome: str                    # nome canonico (= tabela em stg)
    tabela: str                  # tabela na origem
    chave: tuple[str, ...]       # chave natural, em nomes canonicos
    coluna_alteracao: str        # canonico; por convencao dt_alt
    colunas: dict[str, str | dict]   # canonico -> coluna na origem ou {expr: ...}
    pai: Pai | None

    @property
    def canonicos(self) -> tuple[str, ...]:
        return tuple(self.colunas)

    def _expressao(self, canonico: str) -> sql.Composable:
        origem = self.colunas[canonico]
        if isinstance(origem, dict):
            # Escotilha de SQL cru, documentada no YAML. Nao e validada.
            return sql.SQL(origem["expr"])
        return sql.Identifier("t", origem)

    def sql_extracao(self, schema: str, desde: datetime | None
                     ) -> tuple[sql.Composed, list]:
        """SELECT que alimenta o staging.

        desde = None  -> carga completa (le a tabela inteira).
        desde = data  -> incremental: tudo com coluna_alteracao >= desde, MAIS
                         os filhos cujo pai mudou na mesma janela.

        A lista de colunas comeca em sistema_origem e termina em id_execucao
        porque e exatamente a ordem que o COPY para o staging espera; os dois
        entram como parametro, nao como texto no SQL.
        """
        projecao = [sql.Placeholder()]                         # sistema_origem
        projecao += [self._expressao(c) for c in self.canonicos]
        projecao.append(sql.Placeholder())                     # id_execucao

        consulta = sql.SQL("SELECT {campos} FROM {tabela} AS t").format(
            campos=sql.SQL(", ").join(projecao),
            tabela=sql.Identifier(schema, self.tabela),
        )
        parametros: list = [None, None]          # preenchidos por quem executa

        if desde is None:
            return consulta, parametros

        coluna_alt = self.colunas[self.coluna_alteracao]
        if isinstance(coluna_alt, dict):
            raise ConectorInvalido(
                f"{self.nome}: coluna_alteracao nao pode ser uma expressao")

        condicoes = [sql.SQL("t.{col} >= {valor}").format(
            col=sql.Identifier(coluna_alt), valor=sql.Placeholder())]
        parametros.append(desde)

        if self.pai is not None:
            # O filho entra tambem se o PAI mudou na janela, mesmo com o
            # dt_alt do filho antigo. Sem isto o cancelamento de venda (que
            # mexe so no cabecalho) nunca chegaria ao DW.
            condicoes.append(sql.SQL(
                "t.{ligacao} IN (SELECT p.{ligacao_pai} FROM {tabela_pai} AS p "
                "WHERE p.{alt_pai} >= {valor})").format(
                    ligacao=sql.Identifier(self.pai.coluna_ligacao),
                    ligacao_pai=sql.Identifier(self.pai.coluna_ligacao_pai),
                    tabela_pai=sql.Identifier(schema, self.pai.tabela),
                    alt_pai=sql.Identifier(self.pai.coluna_alteracao),
                    valor=sql.Placeholder()))
            parametros.append(desde)

        consulta = consulta + sql.SQL(" WHERE ") + sql.SQL(" OR ").join(condicoes)
        return consulta, parametros


@dataclass(frozen=True)
class Conector:
    sistema_origem: str
    descricao: str
    schema: str
    janela_dias: int
    entidades: dict[str, Entidade]
    caminho: Path

    def entidade(self, nome: str) -> Entidade:
        try:
            return self.entidades[nome]
        except KeyError:
            raise ConectorInvalido(
                f"{self.caminho.name}: entidade {nome!r} nao existe "
                f"(tem: {', '.join(self.entidades)})") from None


def _ler_pai(bruto: dict, onde: str) -> Pai:
    sobrando = set(bruto) - CHAVES_PAI
    if sobrando:
        raise ConectorInvalido(f"{onde}: chave desconhecida {sorted(sobrando)}")
    faltando = CHAVES_PAI - set(bruto)
    if faltando:
        raise ConectorInvalido(f"{onde}: falta {sorted(faltando)}")
    return Pai(
        tabela=_nome(bruto["tabela_pai"], f"{onde}.tabela_pai"),
        coluna_ligacao=_nome(bruto["coluna_ligacao"], f"{onde}.coluna_ligacao"),
        coluna_ligacao_pai=_nome(bruto["coluna_ligacao_pai"],
                                 f"{onde}.coluna_ligacao_pai"),
        coluna_alteracao=_nome(bruto["coluna_alteracao_pai"],
                               f"{onde}.coluna_alteracao_pai"),
    )


def _ler_entidade(nome: str, bruto: dict, arquivo: str) -> Entidade:
    onde = f"{arquivo}: entidades.{nome}"
    if not isinstance(bruto, dict):
        raise ConectorInvalido(f"{onde}: esperado um mapa")
    sobrando = set(bruto) - CHAVES_ENTIDADE
    if sobrando:
        raise ConectorInvalido(f"{onde}: chave desconhecida {sorted(sobrando)}")

    colunas_brutas = bruto.get("colunas")
    if not isinstance(colunas_brutas, dict) or not colunas_brutas:
        raise ConectorInvalido(f"{onde}.colunas: esperado um mapa nao vazio")

    colunas: dict[str, str | dict] = {}
    for canonico, origem in colunas_brutas.items():
        _nome(canonico, f"{onde}.colunas (nome canonico)")
        if isinstance(origem, dict):
            if set(origem) != {"expr"} or not isinstance(origem["expr"], str):
                raise ConectorInvalido(
                    f"{onde}.colunas.{canonico}: mapa so aceita a chave 'expr' "
                    "com uma string")
            colunas[canonico] = origem
        else:
            colunas[canonico] = _nome(origem, f"{onde}.colunas.{canonico}")

    chave_bruta = bruto.get("chave")
    if not isinstance(chave_bruta, list) or not chave_bruta:
        raise ConectorInvalido(f"{onde}.chave: esperado uma lista nao vazia")
    chave = tuple(_nome(c, f"{onde}.chave") for c in chave_bruta)
    for coluna in chave:
        if coluna not in colunas:
            raise ConectorInvalido(
                f"{onde}.chave: {coluna!r} nao esta em colunas")

    coluna_alteracao = _nome(bruto.get("coluna_alteracao"),
                             f"{onde}.coluna_alteracao")
    if coluna_alteracao not in colunas:
        raise ConectorInvalido(
            f"{onde}.coluna_alteracao: {coluna_alteracao!r} nao esta em colunas")

    pai_bruto = bruto.get("reextrair_quando_pai_mudar")
    pai = (_ler_pai(pai_bruto, f"{onde}.reextrair_quando_pai_mudar")
           if pai_bruto is not None else None)

    return Entidade(
        nome=_nome(nome, f"{arquivo}: entidades"),
        tabela=_nome(bruto.get("tabela"), f"{onde}.tabela"),
        chave=chave,
        coluna_alteracao=coluna_alteracao,
        colunas=colunas,
        pai=pai,
    )


def carregar(nome_ou_caminho: str | Path) -> Conector:
    """Le config/conectores/<nome>.yaml (ou um caminho completo) e valida."""
    caminho = Path(nome_ou_caminho)
    if not caminho.suffix:
        caminho = PASTA_CONECTORES / f"{caminho.name}.yaml"
    if not caminho.exists():
        raise ConectorInvalido(f"conector nao encontrado: {caminho}")

    bruto = yaml.safe_load(caminho.read_text(encoding="utf-8"))
    if not isinstance(bruto, dict):
        raise ConectorInvalido(f"{caminho.name}: esperado um mapa no topo")

    entidades_brutas = bruto.get("entidades")
    if not isinstance(entidades_brutas, dict) or not entidades_brutas:
        raise ConectorInvalido(f"{caminho.name}: entidades vazio ou ausente")

    janela = bruto.get("janela_reprocessamento_dias")
    if not isinstance(janela, int) or isinstance(janela, bool) or janela < 0:
        raise ConectorInvalido(
            f"{caminho.name}: janela_reprocessamento_dias deve ser inteiro >= 0")

    return Conector(
        sistema_origem=_nome(bruto.get("sistema_origem"),
                             f"{caminho.name}: sistema_origem"),
        descricao=str(bruto.get("descricao", "")),
        schema=_nome(bruto.get("schema"), f"{caminho.name}: schema"),
        janela_dias=janela,
        entidades={nome: _ler_entidade(nome, corpo, caminho.name)
                   for nome, corpo in entidades_brutas.items()},
        caminho=caminho,
    )
