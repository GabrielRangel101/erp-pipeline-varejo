-- =====================================================================
-- 04_estrela.sql  —  MODELO ESTRELA
-- Banco: dw_varejo   |   Schema: dw   |   Dono: pipeline
--
-- (01_controle, 02_staging e 03_tratada chegam na etapa das cargas; esta
-- etapa é autossuficiente e pode rodar sozinha.)
--
-- Decisões de modelagem:
--  * sistema_origem em toda chave natural: dois ERPs podem usar o mesmo
--    cod_produto/cod_loja sem colidir; integrar um ERP novo não troca chaves.
--  * Chave substituta (sk_*) nas dimensões; fatos guardam as sk e a chave
--    natural. PK natural nos fatos => ON CONFLICT idempotente.
--  * Dimensões em SCD tipo 1 (sobrescreve). Preço e custo históricos já vêm
--    gravados na linha da venda, então o histórico de margem não depende de SCD 2.
--  * fato_venda_item MANTÉM cancelados (cancelado = true) com os valores
--    originais: cancelamento fica mensurável e a conferência fecha
--    (origem = estrela + quarentena). Receita sempre filtra cancelado = false.
--  * Sem linha "desconhecido" nas dimensões: item com produto/loja inexistente
--    NÃO entra na estrela, vai para a quarentena (e é reavaliado a cada carga).
--  * fato_estoque_dia é um retrato DENSO: um registro por dia, loja e produto
--    desde o primeiro movimento do par. Simples e bom para ruptura, mas não
--    escala para milhões de pares x anos (ver README, "Limites conhecidos").
--  * id_execucao liga cada linha à execução que a gravou (tabela de log, criada
--    na etapa de qualidade). Sem FK de propósito: o log não pode travar a carga.
-- =====================================================================

SET ROLE pipeline;
SET client_min_messages = warning;

CREATE SCHEMA IF NOT EXISTS dw;

-- ---------- Dimensões ------------------------------------------------

CREATE TABLE IF NOT EXISTS dw.dim_calendario (
    sk_data              integer      PRIMARY KEY,          -- yyyymmdd
    data                 date         NOT NULL UNIQUE,
    ano                  smallint     NOT NULL,
    trimestre            smallint     NOT NULL,
    mes                  smallint     NOT NULL,
    nome_mes             varchar(12)  NOT NULL,
    semana_iso           smallint     NOT NULL,
    dia                  smallint     NOT NULL,
    dia_semana           smallint     NOT NULL,              -- 1 = segunda ... 7 = domingo
    nome_dia_semana      varchar(15)  NOT NULL,
    eh_fim_de_semana     boolean      NOT NULL,
    eh_feriado_nacional  boolean      NOT NULL DEFAULT false,
    nome_feriado         varchar(40)
);

CREATE TABLE IF NOT EXISTS dw.dim_loja (
    sk_loja          integer      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sistema_origem   varchar(30)  NOT NULL,
    cod_loja         smallint     NOT NULL,
    nome_loja        varchar(80)  NOT NULL,
    cidade           varchar(60),
    uf               char(2),
    formato          varchar(20),
    dt_abertura      date,
    ativa            boolean      NOT NULL,
    dt_alt_origem    timestamp    NOT NULL,
    id_execucao      bigint       NOT NULL,
    dt_carga         timestamptz  NOT NULL DEFAULT now(),
    UNIQUE (sistema_origem, cod_loja)
);

CREATE TABLE IF NOT EXISTS dw.dim_produto (
    sk_produto       integer       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sistema_origem   varchar(30)   NOT NULL,
    cod_produto      integer       NOT NULL,
    cod_barras       varchar(14),
    descricao        varchar(120)  NOT NULL,
    departamento     varchar(40),
    secao            varchar(40),
    unid_medida      char(2),
    cod_fornecedor   integer,
    nome_fornecedor  varchar(120),          -- fornecedor desnormalizado na dimensão (não há dim_fornecedor na fase 1)
    preco_venda      numeric(12,2),
    custo_medio      numeric(12,4),
    ativo            boolean       NOT NULL,
    dt_alt_origem    timestamp     NOT NULL,
    id_execucao      bigint        NOT NULL,
    dt_carga         timestamptz   NOT NULL DEFAULT now(),
    UNIQUE (sistema_origem, cod_produto)
);

-- ---------- Fatos ----------------------------------------------------

-- Grão: um item de venda.
CREATE TABLE IF NOT EXISTS dw.fato_venda_item (
    sistema_origem   varchar(30)   NOT NULL,
    id_item          bigint        NOT NULL,
    id_venda         bigint        NOT NULL,
    sk_data          integer       NOT NULL REFERENCES dw.dim_calendario (sk_data),
    sk_loja          integer       NOT NULL REFERENCES dw.dim_loja (sk_loja),
    sk_produto       integer       NOT NULL REFERENCES dw.dim_produto (sk_produto),
    dt_venda         timestamp     NOT NULL,
    qtd              numeric(12,3) NOT NULL,
    vl_unit          numeric(12,2) NOT NULL,
    vl_bruto         numeric(14,2) NOT NULL,   -- qtd * vl_unit
    vl_desconto      numeric(14,2) NOT NULL,
    vl_liquido       numeric(14,2) NOT NULL,   -- = vl_total da origem (validado: |vl_total - (bruto - desconto)| <= 0,01)
    vl_custo         numeric(14,2) NOT NULL,   -- qtd * vl_custo_unit da origem
    vl_margem        numeric(14,2) NOT NULL,   -- vl_liquido - vl_custo
    cancelado        boolean       NOT NULL,   -- item cancelado OU venda cancelada no cabeçalho
    dt_alt_origem    timestamp     NOT NULL,   -- maior dt_alt entre item e cabeçalho
    id_execucao      bigint        NOT NULL,
    dt_carga         timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (sistema_origem, id_item)
);
CREATE INDEX IF NOT EXISTS ix_fvi_data_loja ON dw.fato_venda_item (sk_data, sk_loja);
CREATE INDEX IF NOT EXISTS ix_fvi_produto   ON dw.fato_venda_item (sk_produto);
CREATE INDEX IF NOT EXISTS ix_fvi_venda     ON dw.fato_venda_item (sistema_origem, id_venda);

-- Grão: um dia x loja x produto (retrato de fim de dia).
CREATE TABLE IF NOT EXISTS dw.fato_estoque_dia (
    sistema_origem     varchar(30)   NOT NULL,
    sk_data            integer       NOT NULL REFERENCES dw.dim_calendario (sk_data),
    sk_loja            integer       NOT NULL REFERENCES dw.dim_loja (sk_loja),
    sk_produto         integer       NOT NULL REFERENCES dw.dim_produto (sk_produto),
    qtd_entrada_compra numeric(14,3) NOT NULL DEFAULT 0,   -- movimentos CMP
    qtd_saida_venda    numeric(14,3) NOT NULL DEFAULT 0,   -- movimentos VND
    qtd_outros         numeric(14,3) NOT NULL DEFAULT 0,   -- líquido de INV, DEV, AJT e PRD (entrada positiva)
    qtd_saldo_final    numeric(14,3) NOT NULL,             -- saldo acumulado ao fim do dia
    em_ruptura         boolean       NOT NULL,             -- qtd_saldo_final <= 0
    id_execucao        bigint        NOT NULL,
    dt_carga           timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (sistema_origem, sk_data, sk_loja, sk_produto)
);
CREATE INDEX IF NOT EXISTS ix_fed_produto ON dw.fato_estoque_dia (sk_produto, sk_data);
CREATE INDEX IF NOT EXISTS ix_fed_loja    ON dw.fato_estoque_dia (sk_loja, sk_data);
