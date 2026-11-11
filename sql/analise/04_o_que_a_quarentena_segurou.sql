-- =====================================================================
-- 04_o_que_a_quarentena_segurou.sql
-- Pergunta: o que o ERP mandou que NAO entrou no numero, e quanto vale?
--
-- Esta e a consulta que um pipeline comum nao consegue responder. Onde o
-- padrao e um JOIN que descarta a linha ruim em silencio, aqui ela tem
-- endereco: trt.quarentena guarda a chave natural, o motivo e ha quantas
-- cargas a linha esta parada. A lista abaixo e a pauta de conversa com quem
-- cuida do ERP - cada motivo aponta para um defeito de cadastro ou de
-- processo do outro lado.
--
-- O valor retido vem do STAGING, nao de uma copia: a quarentena guarda
-- referencia. E e por isso que ela se resolve sozinha quando o cadastro
-- atrasado chega - a linha e relida e reavaliada a cada carga, e tentativas
-- conta ha quantas rodadas ela insiste em falhar.
-- =====================================================================

\echo '== Quarentena por entidade e motivo =='

SELECT q.entidade,
       q.motivo,
       count(*)                                   AS linhas,
       max(q.tentativas)                          AS cargas_insistindo,
       min(q.dt_entrada)::date                    AS primeira_entrada
FROM   trt.quarentena q
GROUP  BY q.entidade, q.motivo
ORDER  BY linhas DESC;

\echo ''
\echo '== Quanto a quarentena esta segurando em dinheiro (itens de venda) =='

SELECT q.motivo,
       count(*)                                   AS linhas,
       round(sum(s.vl_total), 2)                  AS valor_retido,
       round(avg(s.vl_total), 2)                  AS valor_medio
FROM   trt.quarentena q
JOIN   stg.venda_item s
       ON  s.sistema_origem = q.sistema_origem
       AND s.id_item::text  = q.chave
WHERE  q.entidade = 'venda_item'
GROUP  BY q.motivo
ORDER  BY valor_retido DESC NULLS LAST;

\echo ''
\echo '== A conferencia da ultima carga (origem = estrela + quarentena) =='

SELECT c.verificacao,
       count(*)                                   AS grupos_dia_loja,
       count(*) FILTER (WHERE c.bateu)            AS bateram,
       sum(c.qtd_origem)                          AS qtd_origem,
       sum(c.qtd_destino)                         AS qtd_destino,
       sum(c.qtd_quarentena)                      AS qtd_quarentena,
       sum(c.dif_qtd)                             AS dif_qtd,
       sum(c.dif_valor)                           AS dif_valor
FROM   ctl.conferencia c
WHERE  c.id_execucao = (SELECT max(id_execucao) FROM ctl.conferencia)
GROUP  BY c.verificacao
ORDER  BY c.verificacao;
