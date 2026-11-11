-- =====================================================================
-- 01_receita_por_loja_mes.sql
-- Pergunta: quanto cada loja faturou por mes, com que margem e que ticket?
--
-- Por que esta consulta existe no repositorio: ela e a razao de o pipeline
-- existir. Se o numero daqui nao bate com o do ERP, nada mais importa.
--
-- O detalhe que separa esta consulta de um SELECT ingenuo e a ultima coluna.
-- fato_venda_item GUARDA os itens cancelados (cancelado = true) com os valores
-- originais, em vez de apaga-los. Receita filtra cancelado = false; a coluna
-- receita_cancelada mostra quanto um pipeline que ignora o cancelamento de
-- cabecalho estaria somando a mais, todo mes, para sempre.
-- =====================================================================

SELECT
    l.nome_loja,
    to_char(c.data, 'YYYY-MM')                                          AS mes,

    count(DISTINCT f.id_venda) FILTER (WHERE NOT f.cancelado)           AS cupons,
    sum(f.vl_liquido)          FILTER (WHERE NOT f.cancelado)           AS receita,
    sum(f.vl_margem)           FILTER (WHERE NOT f.cancelado)           AS margem,

    round(100 * sum(f.vl_margem)   FILTER (WHERE NOT f.cancelado)
              / nullif(sum(f.vl_liquido) FILTER (WHERE NOT f.cancelado), 0), 1)
                                                                        AS margem_pct,

    round(sum(f.vl_liquido) FILTER (WHERE NOT f.cancelado)
          / nullif(count(DISTINCT f.id_venda) FILTER (WHERE NOT f.cancelado), 0), 2)
                                                                        AS ticket_medio,

    -- o que um pipeline sem tratamento de cancelamento contaria a mais
    coalesce(sum(f.vl_liquido) FILTER (WHERE f.cancelado), 0)           AS receita_cancelada

FROM dw.fato_venda_item f
JOIN dw.dim_loja       l ON l.sk_loja = f.sk_loja
JOIN dw.dim_calendario c ON c.sk_data = f.sk_data
GROUP BY l.nome_loja, to_char(c.data, 'YYYY-MM')
ORDER BY mes, receita DESC;
