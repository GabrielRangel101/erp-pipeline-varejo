-- =====================================================================
-- 02_staging.sql  —  STAGING (copia crua da origem)
-- Banco: dw_varejo   |   Schema: stg   |   Dono: pipeline
--
-- Regras desta camada:
--  1. COPIA CRUA. Nenhuma validacao, nenhuma regra de negocio, nenhuma linha
--     descartada. O que estava na origem esta aqui, inclusive o lixo. Quem
--     julga e a camada tratada.
--  2. NOMES CANONICOS. A origem chama de cad_loja/mov_venda_item; aqui e
--     loja/venda_item. O de-para origem -> staging vive em
--     config/conectores/<erp>.yaml, nao no codigo: integrar um ERP novo e
--     escrever um YAML.
--  3. TIPOS FROUXOS. Texto onde a origem usa dominio (status, uf, unid_medida)
--     e numeric largo nos valores, para caber ERP com layout diferente sem
--     estourar na extracao. Só a chave natural e dt_alt sao NOT NULL.
--  4. PERSISTENTE, NAO TEMPORARIA. O staging NAO e truncado a cada carga: o
--     upsert por chave natural sobrescreve a linha. Isso e o que permite a
--     quarentena ser reavaliada relendo a linha do staging (ver 03_tratada.sql)
--     e o que faz um item cujo cabecalho mudou voltar a ser avaliado sem
--     precisar reler a origem inteira.
--  5. sistema_origem na chave de TUDO: dois ERPs podem usar o mesmo
--     cod_produto sem colidir.
--
-- Limite conhecido: a chave natural e NOT NULL. Um ERP que produza chave nula
-- nao consegue nem ser estagiado; o conector teria que gerar uma chave
-- substituta por expressao no YAML.
--
-- compra e compra_item existem na origem mas NAO sao estagiadas: a estrela da
-- fase 1 nao tem fato de compra. A entrada por compra chega ao DW pelos
-- movimentos CMP de estoque_mov.
-- =====================================================================

SET ROLE pipeline;
SET client_min_messages = warning;

CREATE SCHEMA IF NOT EXISTS stg;

-- ---------- Cadastros ------------------------------------------------

CREATE TABLE IF NOT EXISTS stg.loja (
    sistema_origem  varchar(30)   NOT NULL,
    cod_loja        integer       NOT NULL,
    nome_loja       text,
    cidade          text,
    uf              text,
    formato         text,
    dt_abertura     date,
    ativa           boolean,
    dt_alt          timestamp     NOT NULL,
    id_execucao     bigint        NOT NULL,
    dt_carga        timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (sistema_origem, cod_loja)
);

CREATE TABLE IF NOT EXISTS stg.fornecedor (
    sistema_origem  varchar(30)   NOT NULL,
    cod_fornecedor  integer       NOT NULL,
    razao_social    text,
    uf              text,
    ativo           boolean,
    dt_cadastro     date,
    dt_alt          timestamp     NOT NULL,
    id_execucao     bigint        NOT NULL,
    dt_carga        timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (sistema_origem, cod_fornecedor)
);

CREATE TABLE IF NOT EXISTS stg.produto (
    sistema_origem  varchar(30)   NOT NULL,
    cod_produto     integer       NOT NULL,
    cod_barras      text,
    descricao       text,
    departamento    text,
    secao           text,
    unid_medida     text,
    cod_fornecedor  integer,
    preco_venda     numeric(16,4),
    custo_medio     numeric(16,4),
    ativo           boolean,
    dt_alt          timestamp     NOT NULL,
    id_execucao     bigint        NOT NULL,
    dt_carga        timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (sistema_origem, cod_produto)
);

-- ---------- Vendas ---------------------------------------------------

CREATE TABLE IF NOT EXISTS stg.venda (
    sistema_origem  varchar(30)   NOT NULL,
    id_venda        bigint        NOT NULL,
    cod_loja        integer,
    nr_caixa        integer,
    nr_cupom        integer,
    dt_venda        timestamp,
    status          text,
    vl_bruto        numeric(16,4),
    vl_desconto     numeric(16,4),
    vl_total        numeric(16,4),
    forma_pagto     text,
    dt_alt          timestamp     NOT NULL,
    id_execucao     bigint        NOT NULL,
    dt_carga        timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (sistema_origem, id_venda)
);

CREATE TABLE IF NOT EXISTS stg.venda_item (
    sistema_origem  varchar(30)   NOT NULL,
    id_item         bigint        NOT NULL,
    id_venda        bigint,
    nr_item         integer,
    cod_produto     integer,
    qtd             numeric(16,4),
    vl_unit         numeric(16,4),
    vl_desconto     numeric(16,4),
    vl_total        numeric(16,4),
    vl_custo_unit   numeric(16,4),
    status_item     text,
    dt_alt          timestamp     NOT NULL,
    id_execucao     bigint        NOT NULL,
    dt_carga        timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (sistema_origem, id_item)
);
CREATE INDEX IF NOT EXISTS ix_stg_item_venda ON stg.venda_item (sistema_origem, id_venda);

-- ---------- Estoque --------------------------------------------------

CREATE TABLE IF NOT EXISTS stg.estoque_mov (
    sistema_origem  varchar(30)   NOT NULL,
    id_mov          bigint        NOT NULL,
    cod_loja        integer,
    cod_produto     integer,
    dt_mov          timestamp,
    tp_mov          text,
    sentido         text,
    qtd             numeric(16,4),
    doc_origem      text,
    dt_alt          timestamp     NOT NULL,
    id_execucao     bigint        NOT NULL,
    dt_carga        timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (sistema_origem, id_mov)
);

-- Apoio a reavaliacao da quarentena e ao recorte por execucao.
CREATE INDEX IF NOT EXISTS ix_stg_item_exec    ON stg.venda_item  (id_execucao);
CREATE INDEX IF NOT EXISTS ix_stg_venda_exec   ON stg.venda       (id_execucao);
CREATE INDEX IF NOT EXISTS ix_stg_estoque_exec ON stg.estoque_mov (id_execucao);
