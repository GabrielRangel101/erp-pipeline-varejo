"""
Controle da carga: execucao, marca-d'agua e log de etapas (schema ctl).

Duas regras que valem a pena saber de cor:

1. A MARCA-D'AGUA SO AVANCA NO FIM, E SO SE DEU CERTO. Ela e gravada depois
   que a estrela foi escrita, nao depois da extracao. Se a carga morre no meio,
   a proxima rele a mesma janela; como todo passo e upsert por chave natural,
   reprocessar nao duplica. O contrario (avancar cedo) perderia dado em
   silencio, que e o pior modo de falha de um pipeline.

2. A JANELA DE REPROCESSAMENTO E APLICADA NA LEITURA. O incremental le
   dt_alt >= marca_dagua - N dias. Isso cobre dois casos de uma vez: a linha
   que o ERP gravou com dt_alt retroativo e a corrida entre ler a origem e
   gravar a marca (linha escrita durante a propria carga).
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import psycopg


@dataclass
class Execucao:
    """Uma rodada do pipeline. Carrega o id que carimba toda linha escrita."""

    conn: psycopg.Connection
    sistema_origem: str
    modo: str
    id_execucao: int
    _seq: int = field(default=0, repr=False)

    def registrar_etapa(self, etapa: str, entidade: str | None,
                        linhas_lidas: int | None, linhas_escritas: int | None,
                        duracao_ms: int) -> None:
        self._seq += 1
        self.conn.execute(
            "INSERT INTO ctl.etapa (id_execucao, seq, etapa, entidade, "
            "linhas_lidas, linhas_escritas, duracao_ms) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (self.id_execucao, self._seq, etapa, entidade,
             linhas_lidas, linhas_escritas, duracao_ms))

    @contextmanager
    def cronometrar(self, etapa: str, entidade: str | None = None):
        """Mede o passo e grava em ctl.etapa.

        Devolve um dicionario onde o bloco escreve 'lidas' e 'escritas'. E
        daqui que saem os numeros de docs/resultados.md: medidos pelo proprio
        pipeline, nao cronometrados por fora.
        """
        marca = {"lidas": None, "escritas": None}
        inicio = time.perf_counter()
        try:
            yield marca
        except Exception:
            # A transacao esta abortada: qualquer comando novo so devolve
            # "current transaction is aborted" e o erro DE VERDADE some no
            # meio do caminho. Desfaz primeiro, registra a etapa que quebrou
            # (que e a informacao util no dia seguinte) e deixa o erro subir.
            ms = int((time.perf_counter() - inicio) * 1000)
            self.conn.rollback()
            try:
                self.registrar_etapa(f"{etapa}:FALHA", entidade, marca["lidas"],
                                     marca["escritas"], ms)
                self.conn.commit()
            except Exception:                              # noqa: BLE001
                pass            # nao deixar o log encobrir o erro original
            raise
        else:
            ms = int((time.perf_counter() - inicio) * 1000)
            self.registrar_etapa(etapa, entidade, marca["lidas"],
                                 marca["escritas"], ms)


def abrir_execucao(conn: psycopg.Connection, sistema_origem: str,
                   modo: str) -> Execucao:
    linha = conn.execute(
        "INSERT INTO ctl.execucao (sistema_origem, modo) VALUES (%s, %s) "
        "RETURNING id_execucao", (sistema_origem, modo)).fetchone()
    conn.commit()
    return Execucao(conn, sistema_origem, modo, linha[0])


def fechar_execucao(execucao: Execucao, status: str,
                    mensagem: str | None = None) -> None:
    execucao.conn.execute(
        "UPDATE ctl.execucao SET status = %s, mensagem = %s, dt_fim = now() "
        "WHERE id_execucao = %s",
        (status, mensagem, execucao.id_execucao))
    execucao.conn.commit()


def ler_marca_dagua(conn: psycopg.Connection, sistema_origem: str,
                    entidade: str) -> datetime | None:
    linha = conn.execute(
        "SELECT dt_alt_max FROM ctl.marca_dagua "
        "WHERE sistema_origem = %s AND entidade = %s",
        (sistema_origem, entidade)).fetchone()
    return linha[0] if linha else None


def corte_da_leitura(conn: psycopg.Connection, sistema_origem: str,
                     entidade: str, janela_dias: int) -> datetime | None:
    """A partir de que dt_alt o incremental le esta entidade.

    None significa "le tudo": nunca houve carga desta entidade, entao o
    incremental se comporta como carga completa para ela.
    """
    marca = ler_marca_dagua(conn, sistema_origem, entidade)
    if marca is None:
        return None
    return marca - timedelta(days=janela_dias)


def gravar_marca_dagua(execucao: Execucao, entidade: str,
                       dt_alt_max: datetime | None) -> None:
    """Avanca a marca. dt_alt_max None (nada lido) mantem a marca atual."""
    if dt_alt_max is None:
        return
    execucao.conn.execute(
        "INSERT INTO ctl.marca_dagua "
        "(sistema_origem, entidade, dt_alt_max, id_execucao) "
        "VALUES (%s, %s, %s, %s) "
        "ON CONFLICT (sistema_origem, entidade) DO UPDATE SET "
        "  dt_alt_max = greatest(ctl.marca_dagua.dt_alt_max, EXCLUDED.dt_alt_max), "
        "  id_execucao = EXCLUDED.id_execucao, "
        "  dt_atualizacao = now()",
        (execucao.sistema_origem, entidade, dt_alt_max, execucao.id_execucao))


def limpar_marcas(conn: psycopg.Connection, sistema_origem: str) -> None:
    """Usado pela carga completa: zera as marcas para reler a origem inteira."""
    conn.execute("DELETE FROM ctl.marca_dagua WHERE sistema_origem = %s",
                 (sistema_origem,))
