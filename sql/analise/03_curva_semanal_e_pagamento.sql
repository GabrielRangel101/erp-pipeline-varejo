-- =====================================================================
-- 03_curva_semanal_e_pagamento.sql
-- Pergunta: como a venda se distribui na semana e no mes? (escala de equipe,
-- de caixa e de reposicao sai daqui)
--
-- Esta consulta existe para justificar dim_calendario. Dia da semana sai de
-- extract() sem dimensao nenhuma; feriado, nao. Feriado movel depende da
-- Pascoa e e por isso que o calendario e materializado (ver calendario.py).
--
-- HONESTIDADE SOBRE O DADO: os dados sao sinteticos e o gerador PLANTA uma
-- curva semanal (sexta e sabado puxam) e um pico de inicio de mes (pagamento).
-- Entao esta consulta nao "descobre" nada: o que ela prova e que a curva
-- plantada na origem chega intacta na estrela, depois de passar por staging,
-- tratamento, quarentena e upsert incremental. Se o pipeline distorcesse
-- volume em algum ponto, a curva sairia torta aqui.
--
-- O gerador NAO modela efeito de feriado. A coluna nome_feriado existe para o
-- ERP de verdade; nesta base ela nao tem sinal e por isso nao e analisada.
-- =====================================================================

\echo '== Venda por dia da semana =='

SELECT
    c.dia_semana,
    c.nome_dia_semana,
    count(DISTINCT c.data)                                              AS dias,
    round(sum(f.vl_liquido) / count(DISTINCT c.data), 2)                AS receita_media_dia,
    round(count(DISTINCT f.id_venda)::numeric / count(DISTINCT c.data), 0)
                                                                        AS cupons_media_dia,
    round(sum(f.vl_liquido) / nullif(count(DISTINCT f.id_venda), 0), 2) AS ticket_medio,
    round(100 * (sum(f.vl_liquido) / count(DISTINCT c.data))
              / avg(sum(f.vl_liquido) / count(DISTINCT c.data)) OVER (), 0)
                                                                        AS indice_vs_media
FROM   dw.fato_venda_item f
JOIN   dw.dim_calendario  c ON c.sk_data = f.sk_data
WHERE  NOT f.cancelado
GROUP  BY c.dia_semana, c.nome_dia_semana
ORDER  BY c.dia_semana;

\echo ''
\echo '== Venda por decendio do mes (efeito pagamento) =='

SELECT
    CASE WHEN c.dia <= 10 THEN '01-10'
         WHEN c.dia <= 20 THEN '11-20'
         ELSE                  '21-31' END                              AS faixa_do_mes,
    count(DISTINCT c.data)                                              AS dias,
    round(sum(f.vl_liquido) / count(DISTINCT c.data), 2)                AS receita_media_dia,
    round(sum(f.vl_liquido) / nullif(count(DISTINCT f.id_venda), 0), 2) AS ticket_medio
FROM   dw.fato_venda_item f
JOIN   dw.dim_calendario  c ON c.sk_data = f.sk_data
WHERE  NOT f.cancelado
GROUP  BY 1
ORDER  BY 1;
