"""
Orquestrador da carga.

    python -m pipeline.carga --completa
    python -m pipeline.carga --incremental
    python -m pipeline.carga --incremental --conector erp_a

Ordem fixa, e o motivo de cada passo estar onde esta:

  1. extracao     origem -> stg. Le como erp_ro (SELECT e nada mais).
  2. tratamento   stg -> trt + quarentena. So aqui alguma linha e recusada.
  3. PUBLICACAO   uma transacao so, em conn_dw:
       3a. (completa) esvazia a estrela deste sistema;
       3b. estrela    trt -> dw. Dimensao antes de fato; fato so entra com sk
                      resolvida;
       3c. conferencia  origem == destino + quarentena, por dia e loja, em
                      contagem e em valor. E um PORTAO: se nao fecha, a carga
                      falha aqui;
       3d. marca-d'agua;
       3e. COMMIT.
  4. (completa) VACUUM (ANALYZE) do dw, ja fora da transacao.

POR QUE A PUBLICACAO E UMA TRANSACAO SO
Staging e tratada podem comitar por entidade: ninguem consulta elas. O dw, sim.
Se a estrela comita antes da conferencia e a conferencia reprova, o DW fica
com carga PARCIAL visivel - numero incompleto, sem nada na tela dizendo isso.
E o modo de falha que este projeto existe para recusar.

No Postgres o MVCC ja entrega o que se precisa: quem consulta o DW enxerga o
estado ANTERIOR ate o COMMIT, e enxerga o novo inteiro depois dele. Nunca o
meio. A conferencia roda DENTRO dessa transacao, entao ela ve a estrela nova
(ainda nao comitada) - que e exatamente o que ela tem que julgar.

O custo esta anotado no README, em "Limites conhecidos": o tamanho de uma
carga passa a ser limitado por WAL e espaco, porque nada e liberado antes do
commit.

A extracao COMITA conn_dw (e preciso: a temporaria da COPY cai no commit).
Por isso a transacao de publicacao so pode comecar DEPOIS do tratamento.

MARCA-D'AGUA
Avanca dentro da mesma transacao, no fim. Se a carga morrer em qualquer ponto,
ela fica onde estava e a proxima rodada rele a mesma janela. Como todo passo e
upsert por chave natural, reler nao duplica. Avancar a marca cedo perderia
dado em silencio - o pior modo de falha possivel.

Quando a conferencia falha a marca FICA ONDE ESTAVA de proposito. A proxima
rodada vai reler a mesma janela e falhar de novo, ate alguem olhar. Um
pipeline que destrava sozinho depois de nao fechar a conta so troca erro alto
por erro silencioso.

REGISTRO SOBREVIVE AO ROLLBACK
ctl.execucao, ctl.etapa e ctl.conferencia sao gravados por uma segunda conexao,
em autocommit (ver controle.py). Se fossem pela conexao de dados, o rollback
levaria junto a evidencia de qual dia x loja divergiu.

Codigo de saida: 0 se terminou OK, 1 se falhou. A execucao fica registrada em
ctl.execucao com status FALHA e a mensagem do erro, entao o agendador pode
alarmar sem precisar ler log.
"""

from __future__ import annotations

import argparse
import sys
import time
import traceback
from datetime import date, datetime

import psycopg

from pipeline import conector as mod_conector
from pipeline import conferencia, controle, estrela, extracao, tratamento
from pipeline.conector import Conector
from pipeline.controle import Execucao
from pipeline.db import conectar_dw, conectar_origem_leitura


def _extrair_tudo(conn_origem: psycopg.Connection, conn_dw: psycopg.Connection,
                  conector: Conector, execucao: Execucao,
                  completa: bool) -> dict[str, datetime | None]:
    """Traz todas as entidades para o staging. Devolve a marca nova de cada uma."""
    marcas: dict[str, datetime | None] = {}
    for nome, entidade in conector.entidades.items():
        desde = None if completa else controle.corte_da_leitura(
            conn_dw, conector.sistema_origem, nome, conector.janela_dias)
        with execucao.cronometrar("extracao", nome) as marca:
            lidas, escritas, dt_alt_max = extracao.extrair(
                conn_origem, conn_dw, conector, entidade,
                execucao.id_execucao, desde)
            marca["lidas"] = lidas
            marca["escritas"] = escritas
        marcas[nome] = dt_alt_max
        print(f"  extracao    {nome:<13} lidas={lidas:>9,} desde="
              f"{desde.isoformat(sep=' ', timespec='seconds') if desde else 'tudo'}"
              .replace(",", "."))
    return marcas


def executar(conector: Conector, completa: bool,
             conferir_desde: date | None = None) -> int:
    modo = "completa" if completa else "incremental"
    inicio = time.perf_counter()

    conn_dw = conectar_dw()
    # Conexao separada so para o registro (ctl.execucao, ctl.etapa,
    # ctl.conferencia). Autocommit de proposito: tem que sobreviver ao
    # rollback da transacao de publicacao. Ver controle.py.
    conn_log = conectar_dw(autocommit=True)
    # Retrato unico da origem: a extracao de TODAS as entidades e a
    # conferencia leem o mesmo snapshot. Ver db.py.
    conn_origem = conectar_origem_leitura(snapshot_consistente=True)

    execucao = controle.abrir_execucao(conn_dw, conector.sistema_origem, modo,
                                       conn_log=conn_log)
    print(f"execucao {execucao.id_execucao} | {conector.sistema_origem} | {modo}")

    try:
        if completa:
            # Staging e tratada podem ser limpas e comitadas agora: ninguem
            # consulta elas. A estrela NAO - ela so e tocada dentro da
            # transacao de publicacao, mais abaixo.
            tratamento.limpar_tratada(conn_dw, conector.sistema_origem)
            extracao.limpar_staging(conn_dw, conector)
            conn_dw.commit()
            print("  limpeza     stg e trt zerados; o dw so dentro da publicacao")

        marcas = _extrair_tudo(conn_origem, conn_dw, conector, execucao, completa)

        for resultado in tratamento.tratar(conn_dw, execucao, completa):
            motivos = ", ".join(f"{m}={n}" for m, n in resultado.por_motivo.items())
            print(f"  tratamento  {resultado.entidade:<13} "
                  f"ok={resultado.promovidas:>9,} quarentena={resultado.rejeitadas:>5,} "
                  f"liberadas={resultado.liberadas:>4,}"
                  f"{'  [' + motivos + ']' if motivos else ''}".replace(",", "."))

        # ----- transacao de publicacao: daqui ate o commit, tudo ou nada -----
        if completa:
            # Inclusive as marcas: se a carga reprovar, elas tem que voltar
            # junto com o DW. Apagar as marcas em separado deixaria o proximo
            # incremental lendo a origem inteira sem necessidade.
            metodo = estrela.limpar_estrela(conn_dw, conector.sistema_origem)
            controle.limpar_marcas(conn_dw, conector.sistema_origem)
            print(f"  limpeza     dw: metodo={metodo}")

        alvo = estrela.carregar(conn_dw, execucao, completa)
        print(f"  estrela     dim_loja={alvo.dim_loja} dim_produto={alvo.dim_produto} "
              f"dim_calendario={alvo.dim_calendario}")
        print(f"  estrela     fato_venda_item={alvo.fato_venda_item:,} "
              f"fato_estoque_dia={alvo.fato_estoque_dia:,} "
              f"(desde {alvo.estoque_desde})".replace(",", "."))

        for resultado in conferencia.conferir(conn_origem, conn_dw, execucao,
                                              conferir_desde):
            print(f"  conferencia {resultado.verificacao:<13} "
                  f"{resultado.grupos:,} grupos (dia x loja) conferidos, "
                  f"tudo bate".replace(",", "."))

        # So agora. Ver o cabecalho deste modulo.
        for entidade, dt_alt_max in marcas.items():
            controle.gravar_marca_dagua(execucao, entidade, dt_alt_max)

        conn_dw.commit()
        # ----- fim da transacao de publicacao: agora o DW novo esta visivel --

        if completa:
            # VACUUM nao roda dentro de transacao, por isso e aqui: recolhe o
            # lixo que o DELETE da limpeza deixou e refaz a estatistica.
            estrela.recolher_lixo(conn_dw)

        controle.fechar_execucao(execucao, "OK")
        print(f"OK em {time.perf_counter() - inicio:.2f}s")
        return 0

    except Exception as erro:                              # noqa: BLE001
        # Desfaz a publicacao inteira. O DW volta a ser exatamente o que era
        # antes desta carga; o registro em ctl nao volta, porque vai por
        # conn_log.
        conn_dw.rollback()
        controle.fechar_execucao(execucao, "FALHA",
                                 f"{type(erro).__name__}: {erro}")
        print(f"FALHA na execucao {execucao.id_execucao}: {erro}", file=sys.stderr)
        traceback.print_exc()
        return 1
    finally:
        conn_origem.close()
        conn_dw.close()
        conn_log.close()


def construir_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m pipeline.carga",
        description="Carrega o DW a partir da origem, pelo conector YAML.")
    grupo = p.add_mutually_exclusive_group(required=True)
    grupo.add_argument("--completa", action="store_true",
                       help="apaga o DW deste sistema de origem e recarrega tudo")
    grupo.add_argument("--incremental", action="store_true",
                       help="le so o que mudou desde a marca-d'agua, menos a "
                            "janela de reprocessamento")
    p.add_argument("--conector", default="erp_a",
                   help="nome do conector em config/conectores (padrao: erp_a)")
    p.add_argument("--conferir-desde", type=date.fromisoformat, default=None,
                   metavar="AAAA-MM-DD",
                   help="limita a conferencia a partir desta data. O padrao e "
                        "conferir a historia inteira, que e o que pega a linha "
                        "gravada com dt_alt fora da janela de reprocessamento.")
    return p


def main(argv: list[str] | None = None) -> int:
    args = construir_parser().parse_args(argv)
    try:
        conector = mod_conector.carregar(args.conector)
    except mod_conector.ConectorInvalido as erro:
        print(f"conector invalido: {erro}", file=sys.stderr)
        return 2
    return executar(conector, completa=args.completa,
                    conferir_desde=args.conferir_desde)


if __name__ == "__main__":
    raise SystemExit(main())
