-- =====================================================================
-- 01_papeis_e_bancos.sql
-- Cria papéis e bancos. Roda como superusuário, uma vez, de forma repetível.
-- Variáveis psql (passadas por scripts/inicializar_bancos.sh):
--   :erp_app_pw   senha do "aplicativo do ERP" (dono da origem; usado só pelo gerador)
--   :erp_ro_pw    senha do usuário SOMENTE LEITURA na origem (usado pelo pipeline)
--   :pipeline_pw  senha do dono do DW (usado pelo pipeline)
--
-- Um único servidor Postgres, DOIS bancos: o Postgres não faz consulta entre
-- bancos, então o pipeline é obrigado a extrair pela rede (como seria com um
-- ERP de verdade) e não consegue "colar" na origem por acidente.
-- =====================================================================

-- Papéis (CREATE ROLE não tem IF NOT EXISTS; \gexec executa só se faltar)
SELECT format('CREATE ROLE %I LOGIN', r)
FROM (VALUES ('erp_app'), ('erp_ro'), ('pipeline')) AS t(r)
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = t.r)
\gexec

ALTER ROLE erp_app  PASSWORD :'erp_app_pw';
ALTER ROLE erp_ro   PASSWORD :'erp_ro_pw';
ALTER ROLE pipeline PASSWORD :'pipeline_pw';

-- Camada extra de proteção do usuário de leitura. NÃO é a barreira de segurança:
-- a própria sessão pode desfazer default_transaction_read_only. A barreira real é
-- o GRANT (apenas SELECT), aplicado em sql/origem/01_schema_erp_a.sql e testado
-- em tests/sql/01_origem_seguranca_e_trigger.sql.
ALTER ROLE erp_ro SET default_transaction_read_only = on;
ALTER ROLE erp_ro SET statement_timeout = '10min';
ALTER ROLE erp_ro SET idle_in_transaction_session_timeout = '5min';

-- Rede de seguranca contra transacao esquecida aberta. Mata so quem esta
-- OCIOSO dentro de transacao, nunca quem esta trabalhando, entao nao atrapalha
-- carga longa. Sem isto, um cliente que fez SELECT e nao fechou a transacao
-- segura ACCESS SHARE e qualquer TRUNCATE na origem espera para sempre
-- (aconteceu de verdade na suite de testes da etapa 3).
ALTER ROLE erp_app  SET idle_in_transaction_session_timeout = '5min';
ALTER ROLE pipeline SET idle_in_transaction_session_timeout = '5min';

-- Bancos
SELECT 'CREATE DATABASE erp_origem OWNER erp_app'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'erp_origem')
\gexec

SELECT 'CREATE DATABASE dw_varejo OWNER pipeline'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dw_varejo')
\gexec

-- Horário local do "ERP" (ERPs legados gravam timestamp sem fuso, em horário local).
ALTER DATABASE erp_origem SET timezone TO 'America/Sao_Paulo';

-- Ninguém além dos papéis do projeto entra nos bancos.
REVOKE ALL ON DATABASE erp_origem FROM PUBLIC;
REVOKE ALL ON DATABASE dw_varejo  FROM PUBLIC;
GRANT CONNECT ON DATABASE erp_origem TO erp_app, erp_ro;
GRANT CONNECT ON DATABASE dw_varejo  TO pipeline;
