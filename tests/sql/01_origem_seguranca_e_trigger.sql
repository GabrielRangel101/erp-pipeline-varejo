-- Teste do esquema da origem. Rodar como superusuário; tudo é desfeito no final (ROLLBACK).
--   psql -X -v ON_ERROR_STOP=1 -d erp_origem -f tests/sql/01_origem_seguranca_e_trigger.sql
-- Verifica: (1) trigger de dt_alt, (2) dt_alt antigo informado pela aplicação é respeitado,
--           (3) erp_ro lê mas não escreve nem altera estrutura, (4) ausência de FKs (armadilha deliberada).
BEGIN;
SET LOCAL ROLE erp_app;

INSERT INTO erp.cad_loja (cod_loja, nome_loja, dt_alt) VALUES (999, 'loja teste', '2020-01-01 10:00');

-- (1) UPDATE sem informar dt_alt => o "ERP" carimba a hora atual
UPDATE erp.cad_loja SET nome_loja = 'loja teste 2' WHERE cod_loja = 999;
DO $$
BEGIN
    IF (SELECT dt_alt FROM erp.cad_loja WHERE cod_loja = 999) < current_date THEN
        RAISE EXCEPTION 'FALHA (1): trigger não carimbou dt_alt';
    END IF;
END $$;

-- (2) UPDATE informando dt_alt antigo => respeitado (simula linha que chega atrasada)
UPDATE erp.cad_loja SET nome_loja = 'loja teste 3', dt_alt = '2020-01-02 10:00' WHERE cod_loja = 999;
DO $$
BEGIN
    IF (SELECT dt_alt FROM erp.cad_loja WHERE cod_loja = 999) <> '2020-01-02 10:00' THEN
        RAISE EXCEPTION 'FALHA (2): dt_alt antigo informado pela aplicação não foi respeitado';
    END IF;
END $$;

-- (4) item órfão (venda e produto inexistentes) é aceito: a origem não tem FK
INSERT INTO erp.mov_venda_item (id_venda, cod_produto, qtd, vl_unit, vl_total)
VALUES (-1, -1, 1, 1.00, 1.00);

-- (3) erp_ro: leitura ok; escrita e DDL negadas
RESET ROLE;
SET LOCAL ROLE erp_ro;
DO $$
DECLARE n int;
BEGIN
    SELECT count(*) INTO n FROM erp.cad_loja;          -- leitura precisa funcionar
    IF n < 1 THEN RAISE EXCEPTION 'FALHA (3): erp_ro não leu a tabela'; END IF;

    BEGIN
        INSERT INTO erp.cad_loja (cod_loja, nome_loja) VALUES (998, 'x');
        RAISE EXCEPTION 'FALHA (3): erp_ro conseguiu INSERT';
    EXCEPTION WHEN insufficient_privilege THEN NULL; END;

    BEGIN
        UPDATE erp.cad_loja SET nome_loja = 'x';
        RAISE EXCEPTION 'FALHA (3): erp_ro conseguiu UPDATE';
    EXCEPTION WHEN insufficient_privilege THEN NULL; END;

    BEGIN
        DELETE FROM erp.cad_loja;
        RAISE EXCEPTION 'FALHA (3): erp_ro conseguiu DELETE';
    EXCEPTION WHEN insufficient_privilege THEN NULL; END;

    BEGIN
        TRUNCATE erp.cad_loja;
        RAISE EXCEPTION 'FALHA (3): erp_ro conseguiu TRUNCATE';
    EXCEPTION WHEN insufficient_privilege THEN NULL; END;

    BEGIN
        CREATE TABLE erp.intruso (x int);
        RAISE EXCEPTION 'FALHA (3): erp_ro conseguiu CREATE TABLE';
    EXCEPTION WHEN insufficient_privilege THEN NULL; END;
END $$;

ROLLBACK;
\echo 'OK: trigger de dt_alt, leitura sem escrita para erp_ro e origem sem FK'
