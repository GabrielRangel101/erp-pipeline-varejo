# Pipeline de integração com ERP de varejo (portfólio)

Projeto de portfólio de engenharia de dados do Gabriel Rangel (Rangel Analytics), para candidatura a uma consultoria de BI para supermercados que vende "integração rápida e segura" de ERPs. Este repositório é a prova direta disso.

## Restrições fixas
- DADOS 100% SINTÉTICOS. Nada de ERP real, empresa real ou dado pessoal.
- Orçamento zero: Postgres em Docker, Python, GitHub Actions. Sem dbt, sem serviço pago, sem framework de orquestração. Python e SQL puro (psycopg 3 + PyYAML).
- NÃO acoplar ao dashboard "Mercado Aurora" (isso é a fase 2).
- Nomes em português, sem acento, snake_case. Comentários e README em português.
- Credenciais só por variável de ambiente. Nunca commitar `.env`.

## Regras de trabalho
- NÃO INVENTE RESULTADOS. Tempo, volume e conferência só entram no README se foram medidos rodando o pipeline. Cada número vai para `docs/resultados.md` com comando, data, escala e máquina.
- Aponte direto qualquer decisão frágil, não escalável ou pouco segura, e sugira a melhor prática. Crítica útil vale mais que concordância.
- Cada etapa termina com testes passando e um commit. Não avance com teste vermelho.

## Estado
- Etapa 1 FEITA: `sql/origem/01_schema_erp_a.sql`, `sql/dw/04_estrela.sql`, bootstrap de papéis e bancos, Docker Compose, teste SQL da origem. Validada: 8 tabelas em `erp_origem.erp`, 5 em `dw_varejo.dw`, teste SQL verde.
- Etapa 2 FEITA: `src/pipeline/db.py`, `src/pipeline/catalogo.py`, `src/pipeline/gerador.py` (CLI `--escala` e `--delta`), gabarito `saida/gerador_resumo.json`, 33 testes pytest. Medições em `docs/resultados.md`.
- Etapa 3 FEITA: `sql/dw/01_controle.sql`, `02_staging.sql`, `03_tratada.sql`; `config/conectores/erp_a.yaml`; módulos `conector`, `controle`, `extracao`, `tratamento`, `calendario`, `estrela`, `limpeza` e a CLI `pipeline.carga` (`--completa` / `--incremental`). 91 testes pytest.
- Etapa 4 FEITA: `ctl.conferencia` (em `sql/dw/01_controle.sql`), `src/pipeline/conferencia.py` e o portão em `pipeline.carga`. Duas verificações (`venda_item` contra a estrela, `estoque_mov` contra a tratada), por dia e loja, em contagem e valor, com `--conferir-desde` opcional. Carga que não fecha sai com código 1 e NÃO avança a marca-d'água. 107 testes pytest.
- Etapa 5 FEITA: `.github/workflows/ci.yml`. Sobe o Postgres pelo `docker compose` do projeto (sem secrets, senhas sorteadas no job), roda o teste SQL, o pytest e o ciclo completo na escala pequena — incluindo exigir que a carga incremental com atraso de 15 dias REPROVE. Verde em 1m39s, sem avisos.
- Falta: 6 README + case (Problema / Decisão / Resultado).

## Decisões já tomadas (não reabrir sem motivo)
- Um servidor Postgres, dois bancos: `erp_origem` (schema `erp`) e `dw_varejo` (schemas `ctl`, `stg`, `trt`, `dw`). Extração pela rede, nunca consulta entre bancos.
- Papéis: `erp_app` (dono da origem, só o gerador usa), `erp_ro` (pipeline lê a origem, só SELECT), `pipeline` (dono do DW).
- A origem imita ERP legado de propósito: sem FK, quase sem NOT NULL, timestamps sem fuso em horário local, `dt_alt` mantido por trigger. Armadilhas deliberadas estão no cabeçalho de `01_schema_erp_a.sql`: linha atrasada com `dt_alt` antigo; cancelamento só no cabeçalho da venda (itens ficam 'N' com `dt_alt` antigo); valores nulos, negativos ou com total divergente.
- `sistema_origem` em toda chave natural do DW. O staging usa nomes canônicos; o mapeamento origem -> staging fica em `config/conectores/<erp>.yaml` e integrar ERP novo não pode exigir mexer em código. O YAML precisa suportar extração de itens também pelos cabeçalhos alterados.
- Staging = cópia crua + metadados de carga (id_execucao, dt_carga). Tratada valida e envia linhas ruins para quarentena. A quarentena é reavaliada a cada carga (a linha é relida do staging), pois dimensão que chega tarde não pode virar rejeição permanente.
- Carga incremental: marca-d'água por tabela em `ctl`, upsert `INSERT ... ON CONFLICT`, janela de reprocessamento N = 7 dias (configurável) aplicada sobre `dt_alt`.
- `fato_venda_item` mantém cancelados (`cancelado = true`); receita filtra `cancelado = false`. Estrela: `fato_venda_item`, `fato_estoque_dia` (denso, `em_ruptura` = saldo <= 0), `dim_produto`, `dim_loja`, `dim_calendario` (feriados nacionais).
- Conferência por dia e loja, contagem e soma: origem == estrela + quarentena. Se não bate, a carga FALHA (exit code != 0, log em `ctl`).
- Gerador: escala "pequena" (~50 mil itens, para CI) e "média" (4 lojas, 1.500 produtos, 120 dias, ~1 milhão de itens, para medir). Injeta chegada tardia de até 5 dias, alguns casos de 15 dias (fora da janela, para provar que a conferência falha), cancelamentos só no cabeçalho, órfãos, qtd <= 0 e totais divergentes. Reprodutível por semente.
- Em aberto: segundo ERP sintético (layout diferente) para provar o conector YAML. Recomendado, como etapa extra depois que o ERP A passar.

## Comandos
```bash
cp .env.example .env && docker compose up -d
docker compose exec -T postgres psql -U postgres -d erp_origem -v ON_ERROR_STOP=1 < tests/sql/01_origem_seguranca_e_trigger.sql
pip install -r requirements.txt && pytest
python -m pipeline.gerador --escala pequena --semente 42    # gera a origem
python -m pipeline.gerador --delta --semente 43             # um dia de operacao
python -m pipeline.carga --completa                         # recarrega o DW inteiro
python -m pipeline.carga --incremental                      # so o que mudou
# A carga confere origem == destino + quarentena no fim. Se nao fecha, sai com codigo 1.
```
Python 3.13 (o PyYAML ainda nao tem wheel para 3.15). Rode num venv:
`py -V:3.13 -m venv .venv`. Os testes de banco RECRIAM o conteudo de `erp_origem`.

## README final (etapa 6)
Em português, com: diagrama Mermaid (origem -> staging -> tratada -> estrela -> conferência); "Como eu integraria um ERP novo" (levantar tabelas e chaves, escrever o mapeamento, carga completa, conferência de totais, ligar incremental, liberar agendamento); segurança (usuário somente leitura, credenciais em variáveis, sem dado pessoal, cargas repetíveis sem duplicar); resultados medidos (só os de `docs/resultados.md`); limites conhecidos (estoque denso, FKs no fato, mesmo servidor).
