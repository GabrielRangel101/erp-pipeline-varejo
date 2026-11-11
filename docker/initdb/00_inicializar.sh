#!/usr/bin/env bash
# Executado UMA vez pelo entrypoint do Postgres (volume de dados vazio).
# Os diretórios scripts/ e sql/ são montados em /scripts e /sql pelo docker-compose.
set -euo pipefail
SQL_DIR=/sql bash /scripts/inicializar_bancos.sh
