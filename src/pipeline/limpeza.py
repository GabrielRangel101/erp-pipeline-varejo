"""
Limpeza das camadas antes de uma carga completa.

POR QUE ISTO E UM MODULO E NAO UM "DELETE FROM"

A carga completa precisa apagar so o que e do sistema de origem que esta
sendo recarregado - um DW com dois ERPs nao pode ter a recarga de um apagando
o outro. O jeito obvio e DELETE FROM ... WHERE sistema_origem = %s.

So que DELETE nao devolve espaco. Depois de apagar 1 milhao de linhas a tabela
fica com 1.009.151 tuplas mortas e 224 MB de lixo (medido), que o autovacuum
vai ter que recolher depois. Repetir carga completa varias vezes so faz
crescer.

Entao a escolha e feita na hora:
  * se NENHUMA tabela tem dado de outro sistema de origem, TRUNCATE - devolve
    o espaco na hora e e o caso normal (um DW, um ERP);
  * se tem, DELETE seguido de VACUUM, que ao menos marca as paginas como
    reutilizaveis antes do INSERT comecar.

HONESTIDADE SOBRE O GANHO: trocar DELETE por TRUNCATE NAO acelerou a carga
completa na escala media (127,4 s antes, 130,6 s depois - diferenca dentro do
ruido). A hipotese de que o lixo do DELETE era o gargalo estava ERRADA; o
gargalo e checkpoint/WAL (ver docs/resultados.md). Isto aqui fica por higiene
de espaco em disco, nao por velocidade.

Efeito colateral do TRUNCATE que vale saber: as sk_* das dimensoes sao
IDENTITY e NAO sao reiniciadas, entao uma recarga completa gera chaves
substitutas novas. Isso ja acontecia com o DELETE e e inerente a recarregar
tudo: quem guarda sk por fora precisa recarregar junto.
"""

from __future__ import annotations

import psycopg
from psycopg import sql


def _tem_outro_sistema(conn: psycopg.Connection, schema: str, tabela: str,
                       sistema_origem: str) -> bool:
    return conn.execute(
        sql.SQL("SELECT EXISTS (SELECT 1 FROM {tabela} WHERE sistema_origem <> %s)")
           .format(tabela=sql.Identifier(schema, tabela)),
        (sistema_origem,)).fetchone()[0]


def limpar(conn: psycopg.Connection, schema: str, tabelas: list[str],
           sistema_origem: str) -> str:
    """Esvazia as tabelas para este sistema de origem. Devolve o metodo usado.

    As tabelas vao TODAS no mesmo TRUNCATE porque pode haver chave estrangeira
    entre elas (fato -> dimensao): truncar uma de cada vez seria recusado.
    """
    compartilhado = any(_tem_outro_sistema(conn, schema, t, sistema_origem)
                        for t in tabelas)
    qualificadas = [sql.Identifier(schema, t) for t in tabelas]

    if not compartilhado:
        conn.execute(sql.SQL("TRUNCATE {tabelas}").format(
            tabelas=sql.SQL(", ").join(qualificadas)))
        conn.commit()
        return "truncate"

    for tabela in qualificadas:
        conn.execute(sql.SQL("DELETE FROM {tabela} WHERE sistema_origem = %s")
                     .format(tabela=tabela), (sistema_origem,))
    conn.commit()

    # VACUUM nao roda dentro de transacao; por isso o autocommit temporario.
    anterior = conn.autocommit
    conn.autocommit = True
    try:
        for tabela in qualificadas:
            conn.execute(sql.SQL("VACUUM (ANALYZE) {tabela}").format(tabela=tabela))
    finally:
        conn.autocommit = anterior
    return "delete+vacuum"
