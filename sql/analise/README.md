# Consultas de negocio sobre a estrela

A outra ponta do pipeline: as perguntas que justificam o DW existir. SQL puro,
sem view, sem funcao e sem extensao, para qualquer ferramenta de BI apontar
direto.

| Arquivo | Pergunta | O que a consulta prova sobre o modelo |
|---|---|---|
| [`01_receita_por_loja_mes.sql`](01_receita_por_loja_mes.sql) | Quanto cada loja faturou, com que margem e que ticket? | Por que o fato **guarda** o item cancelado em vez de apaga-lo: a receita filtra, e o cancelado fica mensuravel. |
| [`02_ruptura_de_estoque.sql`](02_ruptura_de_estoque.sql) | Onde a rede perde venda por falta de produto? | Por que `fato_estoque_dia` e **denso**: ruptura e ausencia de venda, e ausencia nao tem linha num fato esparso. |
| [`03_curva_semanal_e_pagamento.sql`](03_curva_semanal_e_pagamento.sql) | Como a venda se distribui na semana e no mes? | Por que existe `dim_calendario`, e que a curva da origem chega intacta na estrela. |
| [`04_o_que_a_quarentena_segurou.sql`](04_o_que_a_quarentena_segurou.sql) | O que o ERP mandou que **nao** entrou no numero, e quanto vale? | Que linha recusada tem endereco, motivo e valor. Nenhum `JOIN` descarta nada em silencio. |

## Rodar

```bash
docker compose exec -T postgres psql -U postgres -d dw_varejo \
    -v ON_ERROR_STOP=1 -f - < sql/analise/01_receita_por_loja_mes.sql
```

A saida real das quatro, medida sobre 1 milhao de itens, esta em
[`docs/analise.md`](../../docs/analise.md) — com o que cada uma mostra e, na
de ruptura, o que esta base **nao** prova.
