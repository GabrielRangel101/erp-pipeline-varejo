# Resultados medidos

Só entra aqui número que saiu de uma execução real, com o comando, a data, a
escala e a máquina. Nada é estimado.

## Máquina de referência (M1)

| Item | Valor |
|---|---|
| CPU | AMD Ryzen 7 5700X, 8 núcleos físicos / 16 lógicos |
| RAM | 15,9 GB |
| Disco | SSD SATA Kingston SA400S37240G |
| Sistema | Windows 10 Pro 22H2 (10.0.19045) |
| Docker | 29.9.3, engine com 16 CPUs e 7,7 GB |
| Postgres | 16.15 (imagem `postgres:16`), `shared_buffers` = 128 MB (padrão) |
| Python | 3.13.17, psycopg 3.3.8 |

O Postgres roda em container com o volume `pgdata` no disco do host; o gerador
roda no host e fala com o banco por TCP em `127.0.0.1:5432`.

---

## Etapa 2 — gerador de dados sintéticos

Data da medição: **2026-11-11**. Todos os comandos abaixo com
`--data-ref 2026-11-11`, na máquina M1.

### Carga inicial

```bash
python -m pipeline.gerador --escala pequena --semente 42 --data-ref 2026-11-11
python -m pipeline.gerador --escala media   --semente 42 --data-ref 2026-11-11
```

| Escala | Dias | Produtos | Tempo | Itens de venda | Linhas escritas | Linhas/s |
|---|---|---|---|---|---|---|
| pequena | 30 | 1.500 | **1,68 s** | 50.074 | 113.155 | ~67 mil |
| media | 120 | 1.500 | **17,76 s** | 1.000.094 | 1.620.081 | ~91 mil |

Contagem por tabela ao fim da carga:

| Tabela | pequena | media |
|---|---:|---:|
| `cad_loja` | 4 | 4 |
| `cad_fornecedor` | 30 | 30 |
| `cad_produto` | 1.500 | 1.500 |
| `mov_venda` | 9.405 | 186.456 |
| `mov_venda_item` | 50.074 | 1.000.094 |
| `mov_estoque` | 44.981 | 402.849 |
| `mov_compra` | 120 | 480 |
| `mov_compra_item` | 7.041 | 28.668 |

Tamanho do banco `erp_origem` na escala média, depois da carga inicial e de um
`--delta` (`pg_database_size`): **267 MB**. As três maiores tabelas, com índices:
`mov_venda_item` 163 MB, `mov_estoque` 53 MB, `mov_venda` 39 MB.

### Modo delta (um dia de operação)

```bash
python -m pipeline.gerador --delta --semente 43 --data-ref 2026-11-11
```

| Base | Tempo | Vendas inseridas | Itens inseridos | Vendas alteradas por UPDATE |
|---|---|---:|---:|---:|
| sobre a pequena | **0,10 s** | 371 | 1.877 | 70 |
| sobre a media | **0,20 s** | 1.711 | 9.199 | 70 |

O volume de vendas novas do dia sai da média dos últimos 7 dias já presentes na
origem: 336 cupons na escala pequena, 1.676 na média.

### Injeções (gabarito `saida/gerador_resumo.json`)

Quantidades da carga inicial (iguais nas duas escalas, exceto o cancelamento,
que é proporcional ao volume) e do delta:

| Tipo | Inicial pequena | Inicial media | Delta |
|---|---:|---:|---:|
| `venda_cancelada_cabecalho` | 49 | 925 | 30 |
| `item_orfao_venda` | 12 | 12 | 3 |
| `item_orfao_produto` | 12 | 12 | 3 |
| `item_qtd_nula` | 8 | 8 | 2 |
| `item_qtd_zero` | 8 | 8 | 2 |
| `item_qtd_negativa` | 8 | 8 | 2 |
| `item_total_divergente` | 20 | 20 | 5 |
| `venda_status_invalido` | 5 | 5 | 2 |
| `venda_loja_nula` | 5 | 5 | 2 |
| `atualizacao_tardia` | — | — | 40 |
| `chegada_tardia_dentro_janela` | — | — | 25 |
| `chegada_tardia_fora_janela` | — | — | 6 |
| `venda_nova_do_dia` | — | — | 336 / 1.676 |

Conferência do gabarito contra a origem em SQL, na escala média depois do
delta (`gabarito == SELECT count(*)` para cada tipo): **os 9 tipos batem**.

### Testes

```bash
pytest
```

**33 testes, 2,63 s**, todos verdes (Python 3.13.17, pytest 9.2.0). Os testes de
banco rodam numa escala própria (12 dias, 200 produtos, 4.000 itens) e recriam
o conteúdo de `erp_origem`.

---

## Etapa 3 — cargas

Data da medição: **2026-11-11**, máquina M1, Postgres com a configuração
**padrão** da imagem (`shared_buffers` 128 MB, `max_wal_size` 1 GB).

```bash
python -m pipeline.carga --completa
python -m pipeline.carga --incremental
```

### Tempo total

| Escala | Modo | Itens processados | Tempo |
|---|---|---:|---|
| pequena | completa | 50.074 | **7,68 s** |
| pequena | incremental | 14.591 | **2,83 s** |
| media | completa | 1.000.094 | **63,78 s** |
| media | incremental | 72.190 | **11,53 s** |

O número que importa para o caso de uso real é o do incremental na escala
média: **um dia de operação sobre um DW de 1 milhão de itens entra em 11,5
segundos**.

A carga completa da escala média levava 127,40 s na primeira versão; caiu para
63,78 s depois do achado do `ANALYZE` descrito adiante.

### Onde o tempo vai (carga completa, escala média)

Medido pelo próprio pipeline, em `ctl.etapa` — execução 60:

| Etapa | Entidade | Linhas | Segundos |
|---|---|---:|---:|
| estrela | fato_venda_item | 1.000.016 | 19,10 |
| estrela | fato_estoque_dia | 720.000 | 17,21 |
| extracao | venda_item | 1.000.094 | 8,03 |
| tratamento | venda_item | 1.000.094 | 7,14 |
| extracao | estoque_mov | 402.849 | 4,62 |
| extracao | venda | 186.456 | 2,95 |
| tratamento | estoque_mov | 402.849 | 2,89 |
| tratamento | venda | 186.456 | 0,97 |

Somando: estrela 36,35 s, extração 15,63 s, tratamento 11,37 s. A estrela
continua sendo a maior fatia (57%), agora dividida de forma equilibrada entre
os dois fatos.

### Resultado da carga completa (escala média)

| Camada | Linhas |
|---|---:|
| `stg.venda_item` | 1.000.094 |
| `trt.venda_item` | 1.000.016 |
| `trt.quarentena` (venda_item) | 78 |
| `dw.fato_venda_item` | 1.000.016 |
| `dw.fato_estoque_dia` | 720.000 |
| `dw.dim_produto` | 1.500 |
| `dw.dim_calendario` | 730 |

Conferência: 1.000.016 + 78 = 1.000.094. **Fecha.** Banco `dw_varejo`: 952 MB.

Quarentena por motivo, na carga completa:
`qtd_invalida` 24, `total_divergente` 20, `produto_inexistente` 12,
`venda_inexistente` 12, `cabecalho_invalido` 10 — e em `venda`,
`loja_inexistente` 5 e `status_invalido` 5. Bate com o gabarito do gerador.

### Testes

**91 testes, 15,54 s**, todos verdes.

---

## Etapa 4 — conferência

Data da medição: **2026-11-11**, máquina M1, configuração padrão do Postgres.

A regra: `origem == destino + quarentena`, por dia e loja, em contagem **e** em
valor. Duas verificações:

| Verificação | Origem | Destino | Valor conferido |
|---|---|---|---|
| `venda_item` | `erp.mov_venda_item` | `dw.fato_venda_item` + quarentena | `vl_total` |
| `estoque_mov` | `erp.mov_estoque` | `trt.estoque_mov` + quarentena | `qtd` |

### Custo

A agregação roda dentro de cada banco; só o resumo por dia e loja atravessa a
rede — 486 linhas na escala média, não 1 milhão.

| Escala | Verificação | Grupos (dia × loja) | Tempo |
|---|---|---:|---:|
| media | `venda_item` | 486 | **2,38 s** |
| media | `estoque_mov` | 480 | **0,14 s** |

Efeito no total da carga completa da escala média: **63,78 s → 69,51 s**. A
conferência da **história inteira**, a cada carga, custa **2,52 s** sobre 1
milhão de itens — 4% do tempo. É barata o bastante para ser o padrão, e é por
isso que ela não foi limitada a "só os dias que a carga tocou".

Num incremental o custo é o mesmo em natureza, mas cai para **0,75 s** porque
as páginas da origem já estão em cache do sistema operacional.

### A conferência falhando, que é o que importa provar

Sequência medida na escala média: carga completa (OK), um dia de operação com
o gerador, carga incremental.

```
FALHA na execucao 138: a conferencia nao fechou: origem != destino + quarentena
  venda_item: 4 de 491 grupos (dia x loja) divergem; faltam 43 linha(s) e 2882.29 em valor
  estoque_mov: 4 de 484 grupos (dia x loja) divergem; faltam 41 linha(s) e 125.933 em valor
  A marca-d'agua NAO foi avancada. [...] rode 'python -m pipeline.carga --completa'.
```

Código de saída **1**, em **14,12 s**. Os 4 grupos divergentes de cada
verificação estão **todos no dia 2026-10-27** — exatamente `data_ref` menos 15
dias, que é a chegada tardia injetada fora da janela de reprocessamento de 7
dias. Nada mais diverge.

Depois disso, medido:

| | Resultado |
|---|---|
| `ctl.execucao` da rodada | `FALHA` |
| marca-d'água | continua na execução anterior |
| rodar o incremental de novo | falha de novo (código 1) |
| rodar `--completa` | **OK em 75,56 s**, 491 e 484 grupos, tudo bate |

Na escala pequena a mesma sequência dá falha em **2,80 s**, com 28 itens
faltando — o número exato de itens das 6 vendas atrasadas.

### Testes

**107 testes, 23,32 s**, todos verdes. Dos 16 novos, 8 existem só para provar
que a conferência **reprova** quando tem que reprovar: um teste que só sabe
dizer "passou" não prova nada.

---

## Etapa 5 — CI no GitHub Actions

Primeira execução: **`main` @ 535ea30, 2026-11-11, verde em 1m21s**
([run 43050504989](https://github.com/GabrielRangel101/erp-pipeline-varejo/actions/runs/43050504989)).
Runner `ubuntu-24.04`, Python 3.13, Postgres 16 subido pelo `docker compose` do
próprio projeto.

| Passo | Tempo |
|---|---:|
| pytest (107 testes) | 26 s |
| Subir o Postgres (pull + healthcheck) | 14 s |
| Gerar a origem e carga completa | 13 s |
| Carga completa recuperando | 11 s |
| Carga incremental reprovando | 5 s |
| Instalar dependências | 4 s |
| Teste SQL, esquema, marca-d'água, limpeza | < 2 s |
| **total do job** | **1m21s** |

O job não se limita a rodar `pytest`. Ele exercita o ciclo inteiro na escala
pequena e **exige que a rede de proteção funcione**: a carga incremental com
atraso de 15 dias tem que reprovar. Se ela passar, o CI falha — porque um
pipeline que para de reclamar é pior do que um que reclama.

Sem nenhum secret: as quatro senhas são sorteadas pelo próprio job, mascaradas
no log e descartadas com o container. Quem clonar o repositório tem CI
funcionando sem configurar nada.

---

## Achados de desempenho

### `PGHOST=localhost` custava 10 s por conexão

Medido nesta máquina, com o compose publicando a porta só em `127.0.0.1`:

| `PGHOST` | Tempo de `psycopg.connect` |
|---|---|
| `localhost` | **10,04 s** |
| `127.0.0.1` | **0,01 s** |

O libpq resolve `localhost` para `::1` antes de `127.0.0.1`, tenta o IPv6
primeiro e só cai para IPv4 depois de estourar o timeout. Como o container
publica apenas em `127.0.0.1`, a tentativa IPv6 sempre falha. O `.env.example`
passou a trazer `PGHOST=127.0.0.1`.

Efeito no tempo total do gerador (mesma carga, mesma semente):

| Comando | Com `localhost` | Com `127.0.0.1` |
|---|---|---|
| `--escala pequena` | 11,74 s | 1,68 s |
| `--delta` | 10,11 s | 0,10 s |

### Dimensão recarregada sem `ANALYZE` custava 57 s por carga completa

O achado mais caro da etapa 3, e o mais fácil de passar batido.

A carga completa trunca `dim_loja` e `dim_produto` e repopula as duas. Logo
depois, o `INSERT` do `fato_venda_item` faz join com elas para resolver as
chaves substitutas. Entre a repopulação e o autoanalyze do Postgres existe uma
janela em que o planner acha que a dimensão está vazia — e escolhe o plano
errado para um join de 1 milhão de linhas.

Como o problema apareceu: o passo `fato_venda_item` levava **78 s** dentro da
carga, mas o **mesmo SQL, isolado, num sistema ocioso e com a tabela truncada,
levava 21 s**. Essa diferença de 3,7× não tinha explicação no SQL.

Duas hipóteses testadas e **descartadas** antes de achar a certa:

| Hipótese | Experimento | Resultado |
|---|---|---|
| Lixo deixado pelo `DELETE` (1.009.151 tuplas mortas, 224 MB) | trocar `DELETE` por `TRUNCATE` | 127,4 s → 130,6 s. **Sem efeito** |
| Pressão de checkpoint/WAL (15 checkpoints forçados) | `shared_buffers` 128 MB → 1 GB, `max_wal_size` 1 GB → 4 GB | 130,6 s → 123,2 s. **Quase sem efeito** |
| **Estatística da dimensão recém-recarregada** | `ANALYZE` na dimensão logo após carregá-la | 130,6 s → **73,3 s** |

Com o `ANALYZE`, o passo `fato_venda_item` caiu de **78,99 s para 21,91 s** —
exatamente o tempo do mesmo SQL medido isolado (21,17 s). A diferença estava
toda aí.

Efeito na carga completa da escala média (configuração padrão do Postgres):

| | Antes | Depois |
|---|---|---|
| total | 130,57 s | **73,34 s** |
| estrela | 96,68 s | 39,70 s |
| `fato_venda_item` | 78,99 s | 21,91 s |

Medindo de novo do zero, com a origem recriada e o `ANALYZE` restrito à carga
completa, a carga completa da escala média ficou em **63,78 s** (contra
**127,40 s** da primeira versão) e o `fato_venda_item` em **19,10 s**.

O mesmo cuidado vale para a camada tratada, que também é recarregada e depois
sofre join: o `ANALYZE` foi colocado nos dois lugares.

**Só na carga completa.** No incremental a tabela não foi truncada, a
estatística continua boa e o `ANALYZE` vira custo fixo puro: medido, ele
levava a carga incremental da escala pequena de **2,13 s para 4,87 s** — mais
que o dobro, sem nenhum ganho. Com a restrição, voltou a **2,83 s**.

Observação honesta: o `TRUNCATE` foi mantido mesmo sem ganho de tempo, porque
`DELETE` deixa 224 MB de espaço morto para o autovacuum recolher depois e isso
cresce a cada recarga. O tuning de WAL foi **revertido**: todos os números
deste documento são com a configuração padrão da imagem `postgres:16`.

### Chave estrangeira no fato custa 2,4× no `INSERT`

Micro-benchmark isolado, inserindo 1.000.016 linhas numa cópia da tabela
(`CREATE TABLE ... LIKE ... INCLUDING ALL`, que copia índices mas não copia FK):

| Cenário | Tempo |
|---|---|
| `INSERT` sem chave estrangeira | **8,72 s** |
| `INSERT` com as 3 chaves estrangeiras | **20,70 s** |
| `INSERT` com as 3 FKs + `ON CONFLICT DO UPDATE` | **22,17 s** |

Cada linha dispara três verificações de FK como gatilho `AFTER ROW`, o que
enfileira 3 milhões de eventos. O `ON CONFLICT` em si é quase de graça
(20,70 → 22,17 s).

As FKs ficam. O custo é ~12 s por carga completa na escala média e elas são a
garantia de que nenhum fato aponta para dimensão inexistente — o teste
`test_fato_so_aponta_para_dimensao_existente` depende disso. Em volume maior o
caminho seria soltá-las durante a carga e revalidar no fim, e é por isso que
"FKs no fato" está em "limites conhecidos" do README.

### `random.choices(weights=...)` dentro do laço

`random.choices` refaz a soma acumulada dos pesos a cada chamada. Com 1.500
produtos e ~186 mil cupons na escala média, isso seria ~280 milhões de somas só
para sortear produto. As somas acumuladas passaram a ser calculadas uma vez e
passadas em `cum_weights`. A medida de 17,76 s já é com a correção; a versão
anterior nunca chegou a ser medida na escala média.
