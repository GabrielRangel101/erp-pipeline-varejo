# A estrela respondendo perguntas de negocio

Um pipeline que ninguem consulta nao provou nada. Este documento e a outra
ponta do repositorio: quatro consultas de negocio rodando sobre a estrela, com
a saida real colada aqui.

As consultas vivem em [`sql/analise/`](../sql/analise). Nenhuma delas usa view,
funcao ou extensao: e SQL que qualquer ferramenta de BI aponta direto.

> **Os dados sao sinteticos.** Os valores em reais sao do gerador deste
> repositorio, nao de supermercado nenhum. O que as consultas provam e que o
> dado atravessa o pipeline inteiro sem se perder e sem se distorcer, e que o
> modelo responde a pergunta certa. Nao sao o diagnostico de uma rede real.

---

## Como foi medido

| Item | Valor |
|---|---|
| Data | 2026-11-11 |
| Maquina | M1 (ver [resultados.md](resultados.md)) |
| Escala | media: 4 lojas, 1.500 produtos, 120 dias |
| `dw.fato_venda_item` | 1.000.016 linhas |
| `dw.fato_estoque_dia` | 720.000 linhas |
| `trt.quarentena` | 78 linhas |

```bash
python -m pipeline.gerador --escala media --semente 42 --data-ref 2026-11-11
python -m pipeline.carga --completa
docker compose exec -T postgres psql -U postgres -d dw_varejo \
    -f - < sql/analise/01_receita_por_loja_mes.sql
```

---

## 1. Receita, margem e ticket por loja e mes

**A pergunta:** quanto cada loja faturou, com que margem, e quanto vale o cupom
medio?

A ultima coluna e o ponto. `fato_venda_item` **guarda** os itens cancelados em
vez de apaga-los, e a receita filtra `cancelado = false`. A coluna
`receita_cancelada` e o que um pipeline que ignora o cancelamento de cabecalho
estaria somando a mais. Todo mes, para sempre, sem ninguem perceber.

```
  nome_loja   |   mes   | cupons |  receita   |   margem   | margem_pct | ticket_medio | receita_cancelada 
--------------+---------+--------+------------+------------+------------+--------------+-------------------
 LOJA CENTRO  | 2026-07 |  10688 | 4309157.40 | 1055871.65 |       24.5 |       403.18 |          14226.83
 LOJA NORTE   | 2026-07 |   6689 | 2716892.68 |  663499.22 |       24.4 |       406.17 |          17655.06
 LOJA SUL     | 2026-07 |   6028 | 2420534.39 |  592397.15 |       24.5 |       401.55 |          13106.57
 LOJA LITORAL | 2026-07 |   3025 | 1240816.98 |  303338.49 |       24.4 |       410.19 |           3355.13
 LOJA CENTRO  | 2026-08 |  19561 | 7926770.41 | 1935386.66 |       24.4 |       405.23 |          36962.42
 LOJA NORTE   | 2026-08 |  12143 | 4927929.54 | 1199441.99 |       24.3 |       405.82 |          29772.07
 LOJA SUL     | 2026-08 |  10919 | 4491679.62 | 1097499.87 |       24.4 |       411.36 |          22130.84
 LOJA LITORAL | 2026-08 |   5453 | 2238179.22 |  546085.34 |       24.4 |       410.45 |          10480.80
 LOJA CENTRO  | 2026-09 |  18602 | 7658846.09 | 1874394.19 |       24.5 |       411.72 |          44987.07
 LOJA NORTE   | 2026-09 |  11730 | 4688437.47 | 1143693.19 |       24.4 |       399.70 |          23462.16
 LOJA SUL     | 2026-09 |  10611 | 4248318.29 | 1041372.04 |       24.5 |       400.37 |          18087.83
 LOJA LITORAL | 2026-09 |   5296 | 2172716.36 |  531882.46 |       24.5 |       410.26 |          11635.56
 LOJA CENTRO  | 2026-10 |  19743 | 8072579.88 | 1981168.86 |       24.5 |       408.88 |          37319.78
 LOJA NORTE   | 2026-10 |  12352 | 5067593.78 | 1242437.50 |       24.5 |       410.27 |          27099.84
 LOJA SUL     | 2026-10 |  11115 | 4570222.51 | 1120242.21 |       24.5 |       411.18 |          18688.72
 LOJA LITORAL | 2026-10 |   5583 | 2254305.81 |  551394.13 |       24.5 |       403.78 |          14588.13
 LOJA CENTRO  | 2026-11 |   6473 | 2659336.36 |  648742.28 |       24.4 |       410.84 |          14137.17
 LOJA NORTE   | 2026-11 |   4066 | 1641494.86 |  400933.66 |       24.4 |       403.71 |           8939.68
 LOJA SUL     | 2026-11 |   3644 | 1495913.26 |  364338.90 |       24.4 |       410.51 |           4596.14
 LOJA LITORAL | 2026-11 |   1800 |  734320.48 |  178915.78 |       24.4 |       407.96 |           6863.14
(20 rows)

Time: 806.909 ms
```

**Leitura:** a margem fica estavel em ~24,4% nas quatro lojas e nos cinco
meses, o que e esperado: o gerador deriva o preco do custo por uma faixa fixa.
O que vale olhar e a ordem de grandeza da ultima coluna. Na LOJA CENTRO, em
outubro, sao R$ 37 mil que **estao no ERP, chegaram na estrela, e nao entram na
receita**. Eles nao sumiram: estao marcados, contaveis e auditaveis.

---

## 2. Ruptura de estoque

**A pergunta:** onde a rede esta perdendo venda por falta de produto?

Esta e a consulta que justifica `fato_estoque_dia` ser **denso** (uma linha por
dia x loja x produto, haja movimento ou nao). Ruptura e a *ausencia* de venda:
num fato esparso, o dia em que o produto acabou simplesmente nao tem linha, e
fica impossivel separar "nao vendeu porque nao tinha" de "nao vendeu porque
ninguem quis".

A venda perdida e uma estimativa conservadora: venda media diaria do par
(loja, produto) nos dias em que **havia** estoque, multiplicada pelos dias em
ruptura.

```
== Ruptura por secao ==
        secao        | dias_produto | dias_em_ruptura | pct_ruptura | pares_afetados | pares_na_estimativa | venda_perdida_estimada 
---------------------+--------------+-----------------+-------------+----------------+---------------------+------------------------
 CAES                |        18720 |            5784 |        30.9 |             77 |                  55 |              987809.70
 GATOS               |        18720 |            3272 |        17.5 |             64 |                  57 |              709714.78
 ALCOOLICAS          |        20160 |            7255 |        36.0 |            102 |                  73 |              598603.91
 PRATOS PRONTOS      |        20160 |            6918 |        34.3 |             98 |                  73 |              532959.89
 NAO ALCOOLICAS      |        33600 |            8598 |        25.6 |            128 |                  93 |              503682.32
 GRAOS E CEREAIS     |        40320 |           12285 |        30.5 |            176 |                 133 |              482171.06
 CASA                |        33600 |           10299 |        30.7 |            145 |                 108 |              479216.04
 LATICINIOS          |        33600 |            8512 |        25.3 |            130 |                  98 |              439843.55
 EMBUTIDOS           |        26880 |            7435 |        27.7 |            116 |                  90 |              419324.12
 BANHO               |        26880 |            7314 |        27.2 |            111 |                  87 |              407596.62
 PAPEL               |        18720 |            5996 |        32.0 |             89 |                  66 |              393988.07
 MATINAIS            |        33600 |            9202 |        27.4 |            141 |                 110 |              389504.84
 VEGETAIS CONGELADOS |        20160 |            5248 |        26.0 |             86 |                  70 |              361707.75
 CAFE E CHA          |        13440 |            3725 |        27.7 |             59 |                  45 |              354901.99
 ORAL                |        18720 |            4229 |        22.6 |             65 |                  52 |              287570.46
 BOVINOS             |        33600 |            3424 |        10.2 |             68 |                  64 |              276050.66
 SORVETES            |        20160 |            5381 |        26.7 |             78 |                  60 |              253908.50
 OLEOS E MOLHOS      |        33600 |            8484 |        25.3 |            134 |                 105 |              217451.53
 MASSAS              |        26880 |            8694 |        32.3 |            113 |                  66 |              194481.18
 CONFEITARIA         |        20160 |            5777 |        28.7 |             80 |                  59 |              194262.42
 LEGUMES             |        33600 |            5677 |        16.9 |             86 |                  61 |              178148.88
 PAES                |        26880 |            7265 |        27.0 |            102 |                  73 |              170546.82
 ROUPAS              |        20160 |            6517 |        32.3 |             94 |                  69 |              170047.19
 AVES                |        20160 |            1971 |         9.8 |             33 |                  26 |              159692.81
 SALGADOS            |        20160 |            4574 |        22.7 |             74 |                  60 |              159556.96
 FRUTAS              |        40320 |            5744 |        14.2 |             84 |                  61 |              142354.13
 VERDURAS            |        26880 |            4298 |        16.0 |             73 |                  62 |              109242.20
 SUINOS              |        20160 |            1888 |         9.4 |             35 |                  29 |               91680.40
(28 rows)

Time: 1078.907 ms (00:01.079)

== Os 10 pares loja x produto que mais custaram (com base suficiente) ==
  nome_loja   |   secao    |            descricao             | dias_ruptura | dias_com_estoque | venda_media_dia | venda_perdida_estimada 
--------------+------------+----------------------------------+--------------+------------------+-----------------+------------------------
 LOJA NORTE   | CAES       | PETISCO CANINO 2UN MERIDIANO     |          100 |               20 |         1205.67 |              120567.15
 LOJA CENTRO  | CAES       | TAPETE HIGIENICO 2UN VERTICE     |           82 |               38 |         1340.54 |              109924.09
 LOJA CENTRO  | GATOS      | RACAO PARA GATOS 1KG DELTA       |           93 |               27 |         1154.92 |              107407.97
 LOJA CENTRO  | ALCOOLICAS | CERVEJA PILSEN 1L GAMA           |          100 |               20 |          960.66 |               96066.00
 LOJA LITORAL | CAES       | RACAO PARA CAES ADULTOS 1KG NOVA |           99 |               21 |          698.15 |               69117.18
 LOJA NORTE   | LATICINIOS | QUEIJO PRATO 2KG ALFA            |           96 |               24 |          679.87 |               65267.84
 LOJA LITORAL | CAES       | RACAO PARA CAES ADULTOS 5KG ALFA |           97 |               23 |          660.31 |               64050.37
 LOJA NORTE   | CAES       | PETISCO CANINO 4UN ORION         |          100 |               20 |          632.90 |               63289.75
 LOJA NORTE   | GATOS      | RACAO PARA GATOS 2KG PRIMA       |           78 |               42 |          749.32 |               58446.74
 LOJA NORTE   | ALCOOLICAS | CERVEJA PILSEN 1L GAMA           |          102 |               18 |          565.13 |               57643.37
(10 rows)

Time: 1051.640 ms (00:01.052)
```

**Leitura:** a diferenca entre `pares_afetados` e `pares_na_estimativa` e
deliberada. Um par que teve estoque em 1 dia de 120 e vendeu bem nesse dia nao
autoriza multiplicar por 119: seria extrapolar uma observacao. Pares com menos
de 15 dias de estoque entram na contagem de ruptura (isso e fato) e ficam fora
da estimativa em dinheiro (isso seria chute). A primeira versao desta consulta
nao fazia esse corte e devolvia R$ 3,2 milhoes so para a secao CAES, quase
todos vindos de pares com um unico dia observado.

### O que esta base nao prova

O nivel de ruptura aqui, de 25% a 36% em varias secoes, **nao e retrato de
supermercado: e defeito do gerador**. Ele compra menos do que vende. Na escala
media sao ~2,5 milhoes de unidades entrando (inventario inicial mais compras)
contra ~3,0 milhoes saindo em venda. O saldo so pode ir para o negativo.

Isso nao afeta o pipeline: a conferencia de `estoque_mov` fecha exatamente
(480 grupos, diferenca zero), porque o que o ERP mandou e o que chegou. Afeta o
realismo do dado. Esta registrado aqui em vez de maquiado porque a regra deste
repositorio e que numero publicado e numero medido, e numero medido vem com o
que ele nao cobre.

---

## 3. Curva semanal e efeito do pagamento

**A pergunta:** como a venda se distribui na semana e no mes? (escala de
equipe, de caixa e de reposicao sai daqui)

Esta consulta existe para justificar `dim_calendario`. Dia da semana sai de
`extract()` sem dimensao nenhuma; feriado, nao. Feriado movel depende da
Pascoa, e e por isso que o calendario e materializado.

```
== Venda por dia da semana ==
 dia_semana | nome_dia_semana | dias | receita_media_dia | cupons_media_dia | ticket_medio | indice_vs_media 
------------+-----------------+------+-------------------+------------------+--------------+-----------------
          1 | segunda-feira   |   17 |         536759.40 |             1312 |       409.01 |              85
          2 | terca-feira     |   18 |         535100.73 |             1309 |       408.65 |              85
          3 | quarta-feira    |   17 |         565966.47 |             1400 |       404.14 |              90
          4 | quinta-feira    |   17 |         601449.78 |             1479 |       406.77 |              95
          5 | sexta-feira     |   17 |         753020.71 |             1848 |       407.56 |             119
          6 | sabado          |   17 |         910883.66 |             2248 |       405.17 |             145
          7 | domingo         |   17 |         508639.53 |             1239 |       410.41 |              81
(7 rows)

Time: 776.309 ms

== Venda por decendio do mes (efeito pagamento) ==
 faixa_do_mes | dias | receita_media_dia | ticket_medio 
--------------+------+-------------------+--------------
 01-10        |   40 |         682494.86 |       406.32
 11-20        |   37 |         616611.35 |       409.28
 21-31        |   43 |         591200.72 |       406.16
(3 rows)

Time: 457.717 ms
```

**Leitura honesta:** o gerador **planta** essa curva. Sexta e sabado puxam, e
ha um pico de inicio de mes. Entao a consulta nao descobre nada. O que ela
prova e que a curva plantada na origem chega **intacta** na estrela depois de
passar por staging, tratamento, quarentena, upsert e carga incremental. Sabado
em 145 e domingo em 81 contra a media e exatamente o formato injetado. Se o
pipeline distorcesse volume em algum ponto (duplicando no upsert, perdendo na
janela incremental) a curva sairia torta aqui, e sairia torta de um jeito
dificil de notar.

O gerador **nao** modela efeito de feriado. A coluna `nome_feriado` existe para
o ERP de verdade; nesta base ela nao tem sinal, e por isso nao e analisada.

---

## 4. O que a quarentena segurou

**A pergunta:** o que o ERP mandou que **nao** entrou no numero, e quanto vale?

Esta e a consulta que um pipeline comum nao consegue responder. Onde o padrao e
um `JOIN` que descarta a linha ruim em silencio, aqui ela tem endereco: chave
natural, motivo, e ha quantas cargas esta parada.

```
== Quarentena por entidade e motivo ==
  entidade  |       motivo        | linhas | cargas_insistindo | primeira_entrada 
------------+---------------------+--------+-------------------+------------------
 venda_item | qtd_invalida        |     24 |                 1 | 2026-11-11
 venda_item | total_divergente    |     20 |                 1 | 2026-11-11
 venda_item | produto_inexistente |     12 |                 1 | 2026-11-11
 venda_item | venda_inexistente   |     12 |                 1 | 2026-11-11
 venda_item | cabecalho_invalido  |     10 |                 1 | 2026-11-11
 venda      | loja_inexistente    |      5 |                 1 | 2026-11-11
 venda      | status_invalido     |      5 |                 1 | 2026-11-11
(7 rows)

Time: 1.421 ms

== Quanto a quarentena esta segurando em dinheiro (itens de venda) ==
       motivo        | linhas | valor_retido | valor_medio 
---------------------+--------+--------------+-------------
 total_divergente    |     20 |      1813.22 |       90.66
 venda_inexistente   |     12 |       358.94 |       29.91
 cabecalho_invalido  |     10 |       190.08 |       19.01
 produto_inexistente |     12 |       120.00 |       10.00
 qtd_invalida        |     24 |      -225.60 |       -9.40
(5 rows)

Time: 51.161 ms

== A conferencia da ultima carga (origem = estrela + quarentena) ==
 verificacao | grupos_dia_loja | bateram | qtd_origem | qtd_destino | qtd_quarentena | dif_qtd | dif_valor 
-------------+-----------------+---------+------------+-------------+----------------+---------+-----------
 estoque_mov |             480 |     480 |     402849 |      402849 |              0 |       0 |    0.0000
 venda_item  |             486 |     486 |    1000094 |     1000016 |             78 |       0 |    0.0000
(2 rows)

Time: 1.101 ms
```

**Leitura:** essa lista e pauta de reuniao com quem cuida do ERP. Cada motivo
aponta para um defeito do outro lado: `produto_inexistente` e cadastro que nao
chegou, `total_divergente` e calculo de desconto errado na frente de caixa,
`venda_inexistente` e item orfao.

Tres detalhes:

- **O valor retido vem do staging**, nao de uma copia. A quarentena guarda
  referencia, e por isso a linha e relida e reavaliada a cada carga: cadastro
  que chega atrasado deixa de ser rejeicao sozinho. `cargas_insistindo` conta ha
  quantas rodadas a linha ainda falha.
- **`qtd_invalida` soma R$ -225,60.** Valor negativo porque sao justamente as
  linhas com quantidade negativa que a regra barra. Essa coluna negativa foi o
  que derrubou uma assercao errada minha no teste de conferencia (contada em
  [construcao.md](construcao.md)): eu tinha suposto que tirar linhas da estrela
  so poderia diminuir a soma.
- **O ultimo bloco e a conferencia fechando.** 486 grupos dia x loja em venda,
  480 em estoque, diferenca zero em contagem e em valor. 1.000.094 linhas na
  origem = 1.000.016 na estrela + 78 na quarentena. E a mesma conta que, quando
  nao fecha, derruba a carga com codigo 1.

---

## Tempo de resposta

Sobre 1.000.016 linhas de venda e 720.000 de estoque, sem nenhum indice alem
dos que `04_estrela.sql` ja cria, sem materializacao e sem cache aquecido:

| Consulta | Tempo |
|---|---|
| 01 - receita por loja e mes | **0,81 s** |
| 02 - ruptura por secao | **1,08 s** |
| 02 - top 10 pares loja x produto | **1,05 s** |
| 03 - curva semanal | **0,78 s** |
| 03 - decendio do mes | **0,46 s** |
| 04 - quarentena por motivo | **0,001 s** |
| 04 - valor retido (join com staging) | **0,05 s** |
| 04 - conferencia da ultima carga | **0,001 s** |

Uma estrela pequena num Postgres de configuracao padrao responde pergunta de
rede de supermercado em menos de dois segundos. O gargalo de um projeto de BI
raramente e o `SELECT`: e a confianca no numero que ele devolve.
