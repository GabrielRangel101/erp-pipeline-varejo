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

DUAS CONEXOES, E O MOTIVO
Execucao carrega duas: `conn`, a conexao de DADOS, e `conn_log`, a conexao de
REGISTRO. A publicacao do DW e uma transacao so em `conn` (ver carga.py), e se
ela falhar o rollback desfaz tudo que estiver nela. Isso e o certo para o dado
e seria desastroso para o log: as linhas de ctl.etapa e de ctl.conferencia da
execucao que falhou sumiriam junto, e elas sao justamente a EVIDENCIA de onde
a conta nao fechou.

Entao a divisao e por natureza da linha, nao por schema:

  ctl.marca_dagua   e ESTADO -> vai em `conn`, dentro da transacao de
                    publicacao. Avancar a marca tem que ser atomico com
                    publicar o dado que ela representa.
  ctl.execucao      e REGISTRO -> vai em `conn_log`, que e autocommit e
  ctl.etapa         sobrevive ao rollback.
  ctl.conferencia

Sem conn_log (o padrao), as duas apontam para a mesma conexao e tudo se
comporta como antes: e o que os testes que chamam abrir_execucao direto usam.
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
    conn_log: psycopg.Connection | None = None
    _seq: int = field(default=0, repr=False)

    @property
    def log(self) -> psycopg.Connection:
        """Onde o registro e gravado. Ver o cabecalho deste modulo."""
        return self.conn_log if self.conn_log is not None else self.conn

    def registrar_etapa(self, etapa: str, entidade: str | None,
                        linhas_lidas: int | None, linhas_escritas: int | None,
                        duracao_ms: int) -> None:
        self._seq += 1
        self.log.execute(
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
            #
            # O rollback aqui tambem e o que desfaz a transacao de publicacao
            # quando quem quebra e um passo dentro dela.
            ms = int((time.perf_counter() - inicio) * 1000)
            self.conn.rollback()
            try:
                self.registrar_etapa(f"{etapa}:FALHA", entidade, marca["lidas"],
                                     marca["escritas"], ms)
                if self.conn_log is None:
                    self.conn.commit()
            except Exception:                              # noqa: BLE001
                pass            # nao deixar o log encobrir o erro original
            raise
        else:
            ms = int((time.perf_counter() - inicio) * 1000)
            self.registrar_etapa(etapa, entidade, marca["lidas"],
                                 marca["escritas"], ms)


def abrir_execucao(conn: psycopg.Connection, sistema_origem: str, modo: str,
                   conn_log: psycopg.Connection | None = None) -> Execucao:
    """Abre a execucao. conn_log, quando dado, recebe todo o REGISTRO."""
    alvo = conn_log if conn_log is not None else conn
    linha = alvo.execute(
        "INSERT INTO ctl.execucao (sistema_origem, modo) VALUES (%s, %s) "
        "RETURNING id_execucao", (sistema_origem, modo)).fetchone()
    if conn_log is None:
        conn.commit()
    return Execucao(conn, sistema_origem, modo, linha[0], conn_log)


def fechar_execucao(execucao: Execucao, status: str,
                    mensagem: str | None = None) -> None:
    execucao.log.execute(
        "UPDATE ctl.execucao SET status = %s, mensagem = %s, dt_fim = now() "
        "WHERE id_execucao = %s",
        (status, mensagem, execucao.id_execucao))
    if execucao.conn_log is None:
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
