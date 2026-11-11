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

### `random.choices(weights=...)` dentro do laço

`random.choices` refaz a soma acumulada dos pesos a cada chamada. Com 1.500
produtos e ~186 mil cupons na escala média, isso seria ~280 milhões de somas só
para sortear produto. As somas acumuladas passaram a ser calculadas uma vez e
passadas em `cum_weights`. A medida de 17,76 s já é com a correção; a versão
anterior nunca chegou a ser medida na escala média.
