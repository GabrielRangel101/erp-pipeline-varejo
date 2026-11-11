-- =====================================================================
-- 01_controle.sql  —  CONTROLE DO PIPELINE
-- Banco: dw_varejo   |   Schema: ctl   |   Dono: pipeline
--
-- Tres tabelas, cada uma com um trabalho:
--   execucao     uma linha por rodada do pipeline. E a unica fonte de id_execucao,
--                que vai carimbado em toda linha de stg, trt e dw: dado qualquer
--                registro do DW, da para dizer que rodada o escreveu e quando.
--   marca_dagua  ate onde a ultima carga leu CADA entidade, medido em dt_alt da
--                origem. A proxima carga le a partir dai, menos a janela de
--                reprocessamento. Por entidade e nao global: uma entidade pode
--                falhar sem travar o avanco das outras.
--   etapa        o que cada passo leu, escreveu e quanto demorou. E daqui que
--                saem os numeros de docs/resultados.md - medidos, nao estimados.
--
-- A marca-d'agua so avanca quando a execucao termina OK (ver src/pipeline/
-- controle.py). Se a carga morre no meio, a proxima relê a mesma janela: o
-- upsert e idempotente, entao reprocessar nao duplica.
-- =====================================================================

SET ROLE pipeline;
SET client_min_messages = warning;

CREATE SCHEMA IF NOT EXISTS ctl;

CREATE TABLE IF NOT EXISTS ctl.execucao (
    id_execucao     bigint       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sistema_origem  varchar(30)  NOT NULL,
    modo            varchar(15)  NOT NULL,    -- completa | incremental
    dt_inicio       timestamptz  NOT NULL DEFAULT now(),
    dt_fim          timestamptz,
    status          varchar(10)  NOT NULL DEFAULT 'RODANDO',  -- RODANDO | OK | FALHA
    mensagem        text,
    CONSTRAINT ck_execucao_modo   CHECK (modo IN ('completa', 'incremental')),
    CONSTRAINT ck_execucao_status CHECK (status IN ('RODANDO', 'OK', 'FALHA'))
);
CREATE INDEX IF NOT EXISTS ix_execucao_inicio ON ctl.execucao (dt_inicio DESC);

CREATE TABLE IF NOT EXISTS ctl.marca_dagua (
    sistema_origem  varchar(30)  NOT NULL,
    entidade        varchar(40)  NOT NULL,    -- nome canonico (loja, venda_item, ...)
    dt_alt_max      timestamp    NOT NULL,    -- maior dt_alt ja lido da origem
    id_execucao     bigint       NOT NULL,
    dt_atualizacao  timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (sistema_origem, entidade)
);

CREATE TABLE IF NOT EXISTS ctl.etapa (
    id_execucao     bigint       NOT NULL REFERENCES ctl.execucao (id_execucao),
    seq             integer      NOT NULL,
    etapa           varchar(40)  NOT NULL,    -- extracao | tratamento | estrela | ...
    entidade        varchar(40),
    linhas_lidas    bigint,
    linhas_escritas bigint,
    duracao_ms      integer      NOT NULL,
    PRIMARY KEY (id_execucao, seq)
);

-- ---------- Conferencia ----------------------------------------------
-- Uma linha por execucao x verificacao x dia x loja, com os TRES lados da
-- conta. A regra e sempre a mesma:
--
--     origem == destino + quarentena
--
-- Guardar os tres lados, e nao so a diferenca, e o que torna a conferencia
-- auditavel depois: da para abrir um dia que falhou e ver de que lado faltou.
-- E guarda TODAS as linhas, nao so as que falharam - conferencia que so
-- registra falha nao prova que chegou a rodar.
--
-- dia e cod_loja sao NULOS de proposito para a linha que nao tem dia nem loja
-- (item orfao, cujo cabecalho nao existe na origem). Ela precisa ser contada
-- em algum lugar, senao "nao sumiu nada" vira so uma frase. Como coluna nula
-- nao entra em PRIMARY KEY, a unicidade vai num indice com coalesce.
CREATE TABLE IF NOT EXISTS ctl.conferencia (
    id_conferencia  bigint        GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_execucao     bigint        NOT NULL REFERENCES ctl.execucao (id_execucao),
    sistema_origem  varchar(30)   NOT NULL,
    verificacao     varchar(40)   NOT NULL,   -- venda_item | estoque_mov
    dia             date,                     -- NULL = linha sem cabecalho
    cod_loja        integer,                  -- NULL = linha sem loja
    qtd_origem      bigint        NOT NULL,
    vl_origem       numeric(20,4) NOT NULL,
    qtd_destino     bigint        NOT NULL,
    vl_destino      numeric(20,4) NOT NULL,
    qtd_quarentena  bigint        NOT NULL,
    vl_quarentena   numeric(20,4) NOT NULL,
    dif_qtd         bigint        NOT NULL,   -- origem - (destino + quarentena)
    dif_valor       numeric(20,4) NOT NULL,
    bateu           boolean       NOT NULL,
    dt_conferencia  timestamptz   NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_conferencia_grao ON ctl.conferencia
    (id_execucao, verificacao,
     coalesce(dia, DATE '0001-01-01'), coalesce(cod_loja, -1));
-- Para a pergunta que o operador faz de manha: quando foi a ultima vez que
-- nao bateu, e onde?
CREATE INDEX IF NOT EXISTS ix_conferencia_falha ON ctl.conferencia (dt_conferencia DESC)
    WHERE NOT bateu;
