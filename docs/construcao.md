# Como este projeto foi construído

Este documento é o "making of". O [README](../README.md) mostra o resultado;
aqui está o caminho — inclusive as voltas erradas, porque elas são a parte
honesta do relato.

O projeto foi construído em seis etapas, cada uma terminando com testes
passando e um commit. Nenhuma etapa avançou com teste vermelho.

---

## A regra que organizou tudo

Antes da primeira linha de código, uma decisão: o pipeline tem que saber
responder **sim ou não** para uma pergunta só.

```
origem == destino + quarentena
```

Por dia e por loja, em contagem e em valor.

Todas as decisões de arquitetura que vieram depois são consequência disso. Não
é uma conferência que foi acrescentada no fim — é a restrição que definiu o
formato das camadas. Staging é persistente *porque* a quarentena precisa reler
a linha. A quarentena guarda referência em vez de cópia *porque* o valor tem
que vir de um lugar só. O fato mantém cancelados *porque* senão a conta não
fecha.

---

## Etapa 1 — O ERP sintético e a estrela

**O que entrou:** o esquema de origem (8 tabelas), o esquema da estrela, os
papéis do banco e o Docker Compose.

A decisão mais importante aqui foi **fazer a origem ruim de propósito**. Um
ERP sintético bem-comportado não prova nada: ele não tem os problemas que
fazem integração dar errado. Então `sql/origem/01_schema_erp_a.sql` imita um
ERP legado de verdade:

- **sem chaves estrangeiras** e quase sem `NOT NULL` — a integridade vive na
  aplicação, por isso existem itens órfãos e valores nulos;
- **`dt_alt` mantido por trigger**, mas a aplicação pode gravar um `dt_alt`
  antigo (importação, contingência fiscal) — é o que cria a linha que chega
  atrasada;
- **cancelar uma venda atualiza só o cabeçalho** — o item continua `'N'` com
  `dt_alt` antigo.

Também aqui ficou decidido: **dois bancos separados**. O Postgres não faz
consulta entre bancos, então a extração é obrigada a passar pela rede. Não é
estética — é o que impede o pipeline de "colar" na origem por acidente.

E três papéis: `erp_app` (dono da origem, só o gerador usa), `erp_ro` (o
pipeline lê, só `SELECT`) e `pipeline` (dono do DW). O teste
`tests/sql/01_origem_seguranca_e_trigger.sql` **tenta escrever** como `erp_ro`
e exige que o banco recuse.

---

## Etapa 2 — O gerador de dados

**O que entrou:** `gerador.py` com duas escalas e dois modos.

- `--escala pequena` (50 mil itens, para o CI) e `--escala media` (1 milhão,
  para medir);
- `--delta`, que simula um dia de operação usando **`UPDATE` de verdade**, para
  a trigger carimbar `dt_alt` como o ERP faria.

A parte que exige cuidado é o **gabarito**: a cada execução o gerador grava
`saida/gerador_resumo.json` com a contagem exata e os ids de cada defeito
injetado. Sem isso, "a quarentena tem 78 linhas" é uma observação; com isso,
é uma afirmação verificável.

### O que deu errado aqui

**Defeitos se sobrepondo.** Itens com `qtd = 0` e `qtd < 0` também contavam
como "total divergente" — 36 linhas onde o gabarito dizia 20. Cada linha
injetada tem que falhar por **um** motivo só, senão a conferência conta a
mesma linha em dois tipos de rejeição. Corrigido fazendo o `vl_total` ser
coerente com a quantidade ruim.

**`PGHOST=localhost` custava 10 segundos por conexão.** O libpq resolve
`localhost` para `::1` antes de `127.0.0.1`, tenta IPv6 primeiro e espera o
timeout inteiro. Como o container publica só em `127.0.0.1`, a tentativa
sempre falhava. O gerador caiu de 11,74 s para 1,68 s com uma linha de
configuração.

**`random.choices(weights=...)` dentro do laço.** Refaz a soma acumulada a cada
chamada: com 1.500 produtos e 186 mil cupons, seriam ~280 milhões de somas só
para sortear produto. Passou a usar `cum_weights` calculado uma vez.

---

## Etapa 3 — As cargas

**O que entrou:** as camadas `ctl`, `stg` e `trt`, o conector YAML, a extração
incremental e a carga da estrela.

O **conector YAML** é a peça que sustenta a promessa "integrar ERP novo não
exige mexer em código". O mapeamento origem → staging vive em
`config/conectores/<erp>.yaml`, e identificadores vindos do YAML são validados
e compostos com `psycopg.sql.Identifier`, nunca por interpolação de string.

A cláusula mais importante do conector é esta:

```yaml
reextrair_quando_pai_mudar:
  tabela_pai: mov_venda
  coluna_ligacao: id_venda
```

Sem ela, cancelamento de venda **nunca** chegaria ao DW, porque o item não foi
tocado e o `dt_alt` dele continua antigo.

A **ordem dos passos** da carga não é arbitrária: extração, tratamento,
estrela e só então a marca-d'água. Ela avança no fim, e só se tudo passou. Se a
carga morre no meio, a próxima relê a mesma janela — e como todo passo é upsert
por chave natural, reler não duplica. Avançar a marca cedo perderia dado em
silêncio.

### O que deu errado aqui

**A suíte de testes travou o banco.** As conexões de teste ficavam `idle in
transaction` segurando `ACCESS SHARE`, e o `TRUNCATE` da geração seguinte
esperava o lock para sempre. A correção foi dupla: conexões de teste em
autocommit, e `idle_in_transaction_session_timeout` nos três papéis — para um
esquecimento desses virar erro em vez de travamento.

**O erro real sumia.** Quando um passo falhava, o log da etapa falhava junto
(transação abortada) e mascarava a causa. Agora o cronômetro desfaz primeiro,
registra a etapa como `:FALHA` e deixa o erro original subir.

**Colisões entre injeções.** As chegadas tardias tinham `dt_venda` de 1 a 5
dias atrás e caíam no sorteio das alterações tardias logo depois — a trigger
recarimbava o `dt_alt` delas e elas deixavam de ser chegada tardia. Resolvido
com uma lista de ids reservados.

### A caçada de performance

A carga completa da escala média levava **127,4 s**, e o passo
`fato_venda_item` sozinho levava 78 s. Mas **o mesmo SQL, isolado, levava 21 s**.
Essa diferença de 3,7× não tinha explicação no SQL.

Duas hipóteses foram testadas e **descartadas**:

| Hipótese | Experimento | Resultado |
|---|---|---|
| Lixo do `DELETE` (1 milhão de tuplas mortas, 224 MB) | trocar por `TRUNCATE` | 127,4 → 130,6 s. Nada |
| Pressão de checkpoint/WAL (15 checkpoints forçados) | `shared_buffers` 8×, `max_wal_size` 4× | 130,6 → 123,2 s. Quase nada |

A causa real: **a carga completa trunca as dimensões e as repopula.** Até o
autoanalyze do Postgres passar, o planner acha que a dimensão está vazia e
escolhe o plano errado para o join de 1 milhão de linhas do fato. Um `ANALYZE`
logo depois de carregar a dimensão derrubou o passo de 78,99 s para 21,91 s —
exatamente o tempo do SQL isolado.

Depois ficou claro que o `ANALYZE` só faz sentido na carga completa: no
incremental a tabela não foi truncada e ele vira custo puro (dobrava a carga
pequena, de 2,13 s para 4,87 s).

O `TRUNCATE` foi mantido mesmo sem ganho de tempo, por higiene de espaço — e o
tuning de WAL foi **revertido**, porque não ajudou e os números do projeto são
com a configuração padrão.

---

## Etapa 4 — A conferência

**O que entrou:** `ctl.conferencia`, o módulo de conferência e o portão na
carga.

Duas verificações, ambas por dia e loja, em contagem e valor:

| Verificação | Origem | Destino |
|---|---|---|
| `venda_item` | `erp.mov_venda_item` | `dw.fato_venda_item` + quarentena |
| `estoque_mov` | `erp.mov_estoque` | `trt.estoque_mov` + quarentena |

Três decisões que valem explicar:

**A tabela guarda os três lados da conta, não só a diferença.** Dá para abrir
um dia que falhou e ver de que lado faltou. E guarda também as linhas que
bateram — conferência que só registra falha não prova que chegou a rodar.

**O item órfão cai no grupo `(NULL, NULL)` dos dois lados.** Ele não tem dia
nem loja, mas precisa ser contado em algum lugar. Sem isso, "não sumiu nada"
seria uma frase, não uma conta.

**Confere a história inteira por padrão.** Conferir só os dias que a carga
tocou deixaria passar justamente a linha gravada com `dt_alt` fora da janela —
que é o caso que a conferência existe para pegar. Custa 2,52 s sobre 1 milhão
de itens; é barato o bastante para ser o padrão.

### O que deu errado aqui

**Uma asserção de teste minha estava errada.** Eu havia escrito que a estrela
nunca somaria mais que a origem num dia — mas itens com quantidade negativa têm
valor negativo, e tirá-los da estrela *aumenta* a soma. O pipeline estava
certo; o teste é que supunha demais. Virou a reconciliação de verdade.

**Corrigi um furo que eu mesmo tinha deixado:** o `--delta` não gerava
movimento de estoque para as chegadas tardias. A venda atrasada entrava sem a
baixa de estoque e a conferência acusaria um furo que o ERP real não tem. Era
mentira de dado sintético, não limite de arquitetura.

**A fixture da etapa 3 quebrou** quando o portão entrou: ela esperava código 0
no incremental. Agora espera 1, porque falhar ali passou a ser o comportamento
certo. Foi uma quebra legítima, não um ajuste para o teste passar.

---

## Etapa 5 — O CI

**O que entrou:** `.github/workflows/ci.yml`.

Duas escolhas fora do idiomático, as duas de propósito:

**Usa o `docker compose` do projeto, não `services:` do Actions.** Assim o CI
percorre exatamente o caminho que está no README, incluindo o bootstrap de
papéis e bancos. README que nunca foi executado é README que não funciona.

**Não usa nenhum secret.** As quatro senhas são sorteadas pelo próprio job,
mascaradas no log e descartadas com o container. O banco vive dentro do runner
e só tem dado sintético. Em troca, quem clonar o repositório tem CI funcionando
sem configurar nada — e o código não muda em produção, porque credencial só
entra por variável de ambiente de qualquer jeito.

O job **não se limita a rodar `pytest`**. Ele exige que a rede de proteção
funcione: a carga incremental com atraso de 15 dias **tem que reprovar**. Se
passar, o CI falha.

### O que deu errado aqui

A primeira execução passou, mas com dois avisos: as actions estavam em v4 e v5
(o atual era v7 nas duas) e o `ubuntu-latest` estava migrando de versão. A
imagem foi fixada em `ubuntu-24.04` — o label mudaria o ambiente embaixo do
pipeline sem ninguém pedir, e quando a imagem for aposentada o CI quebra de uma
vez, que é o modo de falha certo.

Também documentei um run citando o ambiente errado (o runner fixado num run que
ainda usava `ubuntu-latest`) e precisei corrigir.

---

## Etapa 6 — O README

**O que entrou:** o README com o caso, o diagrama, o playbook de integração, a
seção de segurança, os resultados medidos e os limites conhecidos.

Escrever o README **expôs um erro**: o passo a passo não funcionava. `python -m
pipeline.gerador` falhava com `ModuleNotFoundError`, porque o pacote nunca era
instalado — o CI funcionava só porque definia `PYTHONPATH`. Entrou
`pip install -e .` no passo a passo, e o CI deixou de usar o atalho: agora ele
percorre exatamente o caminho documentado. Se o README mentir, o CI quebra.

O diagrama foi feito duas vezes. Primeiro em Mermaid, que só renderiza dentro
do GitHub; depois em SVG desenhado à mão ([docs/pipeline.svg](pipeline.svg)),
que funciona em qualquer lugar — site de portfólio, PDF, apresentação. O
Mermaid ficou no README como fonte editável, dentro de um bloco recolhível.

---

## Regras de trabalho que valeram para todas as etapas

- **Nenhum número inventado.** Tempo, volume e conferência só entram na
  documentação se saíram de uma execução real, e cada um está em
  [docs/resultados.md](resultados.md) com comando, data, escala e máquina.
  Quando o mesmo número foi medido duas vezes com resultados diferentes, os
  dois estão lá.
- **Cada etapa termina com testes passando e um commit.** Nunca avançar com
  teste vermelho.
- **Decisão frágil é apontada, não escondida.** A seção de limites conhecidos
  do README existe porque limite que não está escrito vira surpresa na
  reunião.

---

## O que ficou em aberto

**Um ERP só.** O conector YAML foi desenhado para absorver um layout diferente
sem mexer em código, mas isso é uma afirmação de arquitetura, não um fato
demonstrado — a abstração foi construída olhando para exatamente um exemplo.

Três pontos já identificados que um segundo ERP exporia:

1. **O vocabulário do ERP está no código, não no YAML.** `tratamento.py` tem
   `NOT IN ('F', 'C')` escrito à mão. Um ERP com outras letras para status
   exige mexer em Python.
2. **Chave composta funciona pela metade.** O YAML e o upsert aceitam, mas a
   chave da quarentena está fixada entidade por entidade como coluna única.
3. **A marca-d'água assume timestamp.** ERP que controla alteração por
   sequência numérica não cabe no desenho atual.

Os dois primeiros são dívida; o terceiro é uma decisão de escopo.
