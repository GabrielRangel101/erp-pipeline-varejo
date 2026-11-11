-- =====================================================================
-- 03_tratada.sql  —  CAMADA TRATADA + QUARENTENA
-- Banco: dw_varejo   |   Schema: trt   |   Dono: pipeline
--
-- A tratada e o filtro entre a copia crua e a estrela. Toda linha do staging
-- sai daqui por UMA de duas portas:
--   * passou em todas as regras -> vira linha de trt.<entidade>, tipada e com
--     as medidas ja derivadas (vl_bruto, vl_liquido, vl_margem, cancelado);
--   * reprovou -> vira linha de trt.quarentena, com o CODIGO da regra que
--     falhou e a chave natural da linha reprovada.
--
-- Nao existe terceira porta. Isso e o que faz a conferencia da etapa 4 poder
-- exigir: origem == estrela + quarentena. Linha sumir calado e o modo de falha
-- que este desenho recusa.
--
-- POR QUE A QUARENTENA GUARDA REFERENCIA E NAO COPIA
-- A quarentena guarda (sistema_origem, tabela, chave) + motivo, nao os valores.
-- Os valores sao lidos do staging por join quando precisa. Duas razoes:
--   1. Nao duplica dado: o staging ja e a copia crua e e persistente.
--   2. Reavaliacao de graca. Item rejeitado hoje por "produto_inexistente"
--      porque o cadastro ainda nao tinha chegado passa sozinho na proxima
--      carga: a linha e RELIDA do staging e revalidada. Dimensao que chega
--      tarde nao pode virar rejeicao permanente - e justamente o que acontece
--      num ERP de verdade, onde o cadastro vem num job e o movimento em outro.
--
-- MOTIVOS (vocabulario controlado, sem CHECK de proposito: um ERP novo pode
-- precisar de um motivo novo e isso nao deve exigir migracao)
--   chave_nula            chave natural ausente
--   descricao_ausente     cadastro sem descricao/nome
--   loja_inexistente      cod_loja nulo ou sem linha valida em trt.loja
--   produto_inexistente   cod_produto nulo ou sem linha valida em trt.produto
--   venda_inexistente     item apontando para cabecalho que nao existe (orfao)
--   cabecalho_invalido    o cabecalho da venda esta na quarentena
--   data_invalida         dt_venda / dt_mov nulo
--   status_invalido       status fora do dominio do ERP
--   qtd_invalida          qtd nula, zero ou negativa
--   valor_invalido        vl_unit ou vl_total nulo, ou vl_unit negativo
--   total_divergente      |vl_total - (qtd*vl_unit - vl_desconto)| > 0,01
--   tipo_mov_invalido     tp_mov ou sentido fora do dominio
-- =====================================================================

SET ROLE pipeline;
SET client_min_messages = warning;

CREATE SCHEMA IF NOT EXISTS trt;

-- ---------- Quarentena -----------------------------------------------

CREATE TABLE IF NOT EXISTS trt.quarentena (
    sistema_origem       varchar(30)  NOT NULL,
    entidade             varchar(40)  NOT NULL,   -- nome canonico: venda_item, produto, ...
    chave                varchar(60)  NOT NULL,   -- chave natural em texto
    motivo               varchar(40)  NOT NULL,   -- primeira regra que falhou
    detalhe              text,
    id_execucao_entrada  bigint       NOT NULL,   -- rodada que rejeitou pela 1a vez
    dt_entrada           timestamptz  NOT NULL DEFAULT now(),
    id_execucao_ultima   bigint       NOT NULL,   -- rodada que reavaliou por ultimo
    dt_ultima            timestamptz  NOT NULL DEFAULT now(),
    tentativas           integer      NOT NULL DEFAULT 1,
    PRIMARY KEY (sistema_origem, entidade, chave)
);
CREATE INDEX IF NOT EXISTS ix_quarentena_motivo ON trt.quarentena (entidade, motivo);

-- ---------- Cadastros tratados ---------------------------------------

CREATE TABLE IF NOT EXISTS trt.loja (
    sistema_origem  varchar(30)  NOT NULL,
    cod_loja        integer      NOT NULL,
    nome_loja       varchar(80)  NOT NULL,
    cidade          varchar(60),
    uf              char(2),
    formato         varchar(20),
    dt_abertura     date,
    ativa           boolean      NOT NULL,
    dt_alt_origem   timestamp    NOT NULL,
    id_execucao     bigint       NOT NULL,
    dt_carga        timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (sistema_origem, cod_loja)
);

CREATE TABLE IF NOT EXISTS trt.produto (
    sistema_origem  varchar(30)   NOT NULL,
    cod_produto     integer       NOT NULL,
    cod_barras      varchar(14),
    descricao       varchar(120)  NOT NULL,
    departamento    varchar(40),
    secao           varchar(40),
    unid_medida     char(2),
    cod_fornecedor  integer,
    nome_fornecedor varchar(120),             -- desnormalizado: nao ha dim_fornecedor na fase 1
    preco_venda     numeric(12,2),
    custo_medio     numeric(12,4),
    ativo           boolean       NOT NULL,
    dt_alt_origem   timestamp     NOT NULL,
    id_execucao     bigint        NOT NULL,
    dt_carga        timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (sistema_origem, cod_produto)
);

-- ---------- Vendas tratadas ------------------------------------------

-- Cabecalho validado. Nao vira fato (o grao da estrela e o item), mas o item
-- depende dele para loja, data e cancelamento.
CREATE TABLE IF NOT EXISTS trt.venda (
    sistema_origem  varchar(30)  NOT NULL,
    id_venda        bigint       NOT NULL,
    cod_loja        integer      NOT NULL,
    dt_venda        timestamp    NOT NULL,
    cancelada       boolean      NOT NULL,
    dt_alt_origem   timestamp    NOT NULL,
    id_execucao     bigint       NOT NULL,
    dt_carga        timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (sistema_origem, id_venda)
);

CREATE TABLE IF NOT EXISTS trt.venda_item (
    sistema_origem  varchar(30)   NOT NULL,
    id_item         bigint        NOT NULL,
    id_venda        bigint        NOT NULL,
    cod_loja        integer       NOT NULL,
    cod_produto     integer       NOT NULL,
    dt_venda        timestamp     NOT NULL,
    qtd             numeric(12,3) NOT NULL,
    vl_unit         numeric(12,2) NOT NULL,
    vl_bruto        numeric(14,2) NOT NULL,
    vl_desconto     numeric(14,2) NOT NULL,
    vl_liquido      numeric(14,2) NOT NULL,
    vl_custo        numeric(14,2) NOT NULL,
    vl_margem       numeric(14,2) NOT NULL,
    cancelado       boolean       NOT NULL,   -- item cancelado OU cabecalho cancelado
    dt_alt_origem   timestamp     NOT NULL,   -- maior dt_alt entre item e cabecalho
    id_execucao     bigint        NOT NULL,
    dt_carga        timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (sistema_origem, id_item)
);
CREATE INDEX IF NOT EXISTS ix_trt_item_dia ON trt.venda_item (dt_venda, cod_loja);

-- ---------- Estoque tratado ------------------------------------------

CREATE TABLE IF NOT EXISTS trt.estoque_mov (
    sistema_origem  varchar(30)   NOT NULL,
    id_mov          bigint        NOT NULL,
    cod_loja        integer       NOT NULL,
    cod_produto     integer       NOT NULL,
    dt_mov          timestamp     NOT NULL,
    tp_mov          varchar(3)    NOT NULL,
    sentido         char(1)       NOT NULL,
    qtd             numeric(14,3) NOT NULL,
    dt_alt_origem   timestamp     NOT NULL,
    id_execucao     bigint        NOT NULL,
    dt_carga        timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (sistema_origem, id_mov)
);
CREATE INDEX IF NOT EXISTS ix_trt_mov_dia ON trt.estoque_mov (dt_mov);
