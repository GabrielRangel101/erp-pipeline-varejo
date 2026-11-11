"""
Orquestrador da carga.

    python -m pipeline.carga --completa
    python -m pipeline.carga --incremental
    python -m pipeline.carga --incremental --conector erp_a

Ordem fixa, e o motivo de cada passo estar onde esta:

  1. extracao     origem -> stg. Le como erp_ro (SELECT e nada mais).
  2. tratamento   stg -> trt + quarentena. So aqui alguma linha e recusada.
  3. estrela      trt -> dw. Dimensao antes de fato; fato so entra com sk
                  resolvida.
  4. conferencia  origem == destino + quarentena, por dia e loja, em contagem
                  e em valor. E um PORTAO: se nao fecha, a carga falha aqui.
  5. marca-d'agua avanca SO AGORA, e so se os quatro passos acima passaram. Se a
                  carga morrer em qualquer ponto, a marca fica onde estava e a
                  proxima rodada rele a mesma janela. Como todo passo e upsert
                  por chave natural, reler nao duplica. Avancar a marca cedo
                  perderia dado em silencio - o pior modo de falha possivel.

Codigo de saida: 0 se terminou OK, 1 se falhou. A execucao fica registrada em
ctl.execucao com status FALHA e a mensagem do erro, entao o agendador pode
alarmar sem precisar ler log.

Quando a conferencia falha a marca-d'agua FICA ONDE ESTAVA de proposito. A
proxima rodada vai reler a mesma janela e falhar de novo, ate alguem olhar.
Um pipeline que destrava sozinho depois de nao fechar a conta so troca erro
alto por erro silencioso.

O que a carga ja escreveu no DW NAO e desfeito nesse caso. A conferencia roda
depois da estrela, e cada passo comita o seu pedaco (senao uma carga de 1
milhao de linhas viraria uma transacao gigante). O dado que entrou esta
certo - so esta INCOMPLETO. Quem impede isso de virar silencio sao as duas
outras coisas: a marca-d'agua parada e o status FALHA em ctl.execucao. A saida
e 'python -m pipeline.carga --completa'.
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
    conn_origem = conectar_origem_leitura()
    execucao = controle.abrir_execucao(conn_dw, conector.sistema_origem, modo)
    print(f"execucao {execucao.id_execucao} | {conector.sistema_origem} | {modo}")

    try:
        if completa:
            # Zera de tras para frente para nao deixar orfao em nenhum ponto.
            metodo = estrela.limpar_estrela(conn_dw, conector.sistema_origem)
            tratamento.limpar_tratada(conn_dw, conector.sistema_origem)
            extracao.limpar_staging(conn_dw, conector)
            controle.limpar_marcas(conn_dw, conector.sistema_origem)
            conn_dw.commit()
            print(f"  limpeza     metodo={metodo} "
                  f"(delete+vacuum quando o DW tem outro sistema de origem)")

        marcas = _extrair_tudo(conn_origem, conn_dw, conector, execucao, completa)

        for resultado in tratamento.tratar(conn_dw, execucao, completa):
            motivos = ", ".join(f"{m}={n}" for m, n in resultado.por_motivo.items())
            print(f"  tratamento  {resultado.entidade:<13} "
                  f"ok={resultado.promovidas:>9,} quarentena={resultado.rejeitadas:>5,} "
                  f"liberadas={resultado.liberadas:>4,}"
                  f"{'  [' + motivos + ']' if motivos else ''}".replace(",", "."))

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

        controle.fechar_execucao(execucao, "OK")
        print(f"OK em {time.perf_counter() - inicio:.2f}s")
        return 0

    except Exception as erro:                              # noqa: BLE001
        conn_dw.rollback()
        controle.fechar_execucao(execucao, "FALHA",
                                 f"{type(erro).__name__}: {erro}")
        print(f"FALHA na execucao {execucao.id_execucao}: {erro}", file=sys.stderr)
        traceback.print_exc()
        return 1
    finally:
        conn_origem.close()
        conn_dw.close()


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
