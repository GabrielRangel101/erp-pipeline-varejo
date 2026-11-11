#!/usr/bin/env bash
# Cria papéis e bancos e aplica os esquemas da origem e da estrela.
# Repetível: pode rodar de novo sem quebrar nem apagar dados.
#
# Conexão de superusuário pelas variáveis padrão do libpq:
#   PGHOST, PGPORT, PGUSER (padrão: postgres), PGPASSWORD
# Senhas dos papéis do projeto (obrigatórias):
#   ERP_APP_PASSWORD, ERP_RO_PASSWORD, PIPELINE_PASSWORD
set -euo pipefail

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SQL_DIR="${SQL_DIR:-$RAIZ/sql}"

: "${ERP_APP_PASSWORD:?defina ERP_APP_PASSWORD}"
: "${ERP_RO_PASSWORD:?defina ERP_RO_PASSWORD}"
: "${PIPELINE_PASSWORD:?defina PIPELINE_PASSWORD}"

psql_su() { psql -X -v ON_ERROR_STOP=1 "$@"; }

echo ">> papéis e bancos"
psql_su -d postgres \
  -v erp_app_pw="$ERP_APP_PASSWORD" \
  -v erp_ro_pw="$ERP_RO_PASSWORD" \
  -v pipeline_pw="$PIPELINE_PASSWORD" \
  -f "$SQL_DIR/00_bootstrap/01_papeis_e_bancos.sql"

echo ">> esquema da origem (ERP A)"
psql_su -d erp_origem -f "$SQL_DIR/origem/01_schema_erp_a.sql"

echo ">> esquema do DW"
# Etapas seguintes acrescentam aqui 01_controle, 02_staging e 03_tratada.
for f in "$SQL_DIR"/dw/[0-9][0-9]_*.sql; do
  echo "   $(basename "$f")"
  psql_su -d dw_varejo -f "$f"
done

echo ">> ok"
