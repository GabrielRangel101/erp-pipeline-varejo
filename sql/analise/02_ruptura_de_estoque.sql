-- =====================================================================
-- 02_ruptura_de_estoque.sql
-- Pergunta: onde o supermercado esta perdendo venda por falta de produto?
--
-- Esta e a consulta que justifica fato_estoque_dia ser DENSO (uma linha por
-- dia x loja x produto desde o primeiro movimento do par, haja movimento ou
-- nao). Ruptura e a AUSENCIA de venda. Num fato esparso, o dia em que o
-- produto acabou simplesmente nao tem linha, e e impossivel distinguir
-- "nao vendeu porque nao tinha" de "nao vendeu porque ninguem quis".
--
-- O custo dessa escolha esta no README, em "Limites conhecidos": o fato denso
-- cresce por dias x lojas x produtos, nao por movimento.
--
-- A venda perdida e uma ESTIMATIVA conservadora: venda media diaria do par
-- (loja, produto) nos dias em que HAVIA estoque, multiplicada pelos dias em
-- ruptura. Nao modela substituicao entre produtos nem demanda reprimida.
--
-- MINIMO_DIAS_OBSERVADOS existe porque a estimativa so vale se houver base
-- para ela. Um par que teve estoque em 1 dia dos 120 e vendeu bem nesse dia
-- nao autoriza multiplicar por 119: seria extrapolar uma observacao. Pares
-- abaixo do minimo entram na contagem de ruptura (isso e fato) e ficam de
-- fora da estimativa de dinheiro (isso seria chute).
--
-- LIMITE CONHECIDO DESTA BASE: o gerador sintetico compra MENOS do que vende
-- (entradas ~2,5 milhoes de unidades contra ~3,0 milhoes de saidas na escala
-- media). O nivel de ruptura que aparece aqui e artefato do gerador, nao
-- retrato de supermercado. O que a consulta demonstra e o CAMINHO - a
-- pergunta de negocio que o modelo denso responde - e nao o diagnostico.
-- =====================================================================

\set MINIMO_DIAS_OBSERVADOS 15

\echo '== Ruptura por secao =='

WITH venda_dia AS (
    SELECT sk_loja, sk_produto, sk_data, sum(vl_liquido) AS vl
    FROM   dw.fato_venda_item
    WHERE  NOT cancelado
    GROUP  BY 1, 2, 3
),
por_par AS (
    SELECT e.sk_loja,
           e.sk_produto,
           count(*) FILTER (WHERE e.em_ruptura)                    AS dias_ruptura,
           count(*) FILTER (WHERE NOT e.em_ruptura)                AS dias_com_estoque,
           avg(coalesce(v.vl, 0)) FILTER (WHERE NOT e.em_ruptura)  AS vl_dia_normal
    FROM   dw.fato_estoque_dia e
    LEFT   JOIN venda_dia v USING (sk_loja, sk_produto, sk_data)
    GROUP  BY 1, 2
)
SELECT p.secao,
       sum(j.dias_ruptura + j.dias_com_estoque)                    AS dias_produto,
       sum(j.dias_ruptura)                                         AS dias_em_ruptura,
       round(100.0 * sum(j.dias_ruptura)
             / nullif(sum(j.dias_ruptura + j.dias_com_estoque), 0), 1)
                                                                   AS pct_ruptura,
       count(*) FILTER (WHERE j.dias_ruptura > 0)                  AS pares_afetados,
       count(*) FILTER (WHERE j.dias_ruptura > 0
                        AND   j.dias_com_estoque >= :MINIMO_DIAS_OBSERVADOS)
                                                                   AS pares_na_estimativa,
       round(sum(j.dias_ruptura * j.vl_dia_normal)
             FILTER (WHERE j.dias_ruptura > 0
                     AND   j.dias_com_estoque >= :MINIMO_DIAS_OBSERVADOS), 2)
                                                                   AS venda_perdida_estimada
FROM   por_par j
JOIN   dw.dim_produto p ON p.sk_produto = j.sk_produto
GROUP  BY p.secao
ORDER  BY venda_perdida_estimada DESC NULLS LAST;

\echo ''
\echo '== Os 10 pares loja x produto que mais custaram (com base suficiente) =='

WITH venda_dia AS (
    SELECT sk_loja, sk_produto, sk_data, sum(vl_liquido) AS vl
    FROM   dw.fato_venda_item
    WHERE  NOT cancelado
    GROUP  BY 1, 2, 3
),
por_par AS (
    SELECT e.sk_loja,
           e.sk_produto,
           count(*) FILTER (WHERE e.em_ruptura)                    AS dias_ruptura,
           count(*) FILTER (WHERE NOT e.em_ruptura)                AS dias_com_estoque,
           avg(coalesce(v.vl, 0)) FILTER (WHERE NOT e.em_ruptura)  AS vl_dia_normal
    FROM   dw.fato_estoque_dia e
    LEFT   JOIN venda_dia v USING (sk_loja, sk_produto, sk_data)
    GROUP  BY 1, 2
)
SELECT l.nome_loja,
       p.secao,
       p.descricao,
       j.dias_ruptura,
       j.dias_com_estoque,
       round(j.vl_dia_normal, 2)                                   AS venda_media_dia,
       round(j.dias_ruptura * j.vl_dia_normal, 2)                  AS venda_perdida_estimada
FROM   por_par j
JOIN   dw.dim_produto p ON p.sk_produto = j.sk_produto
JOIN   dw.dim_loja    l ON l.sk_loja    = j.sk_loja
WHERE  j.dias_ruptura    > 0
  AND  j.dias_com_estoque >= :MINIMO_DIAS_OBSERVADOS
ORDER  BY venda_perdida_estimada DESC
LIMIT  10;
