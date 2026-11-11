# Perguntas sobre o desenho

Toda decisão deste repositório teve uma alternativa razoável que foi
descartada. Aqui estão as perguntas que um revisor faria, com a resposta e o
que foi descartado em cada uma.

---

### Por que o staging é persistente, em vez de uma tabela temporária?

Porque a quarentena guarda **referência**, não cópia. `trt.quarentena` grava
`(sistema_origem, entidade, chave, motivo)` — não os valores. Os valores são
lidos do staging por `JOIN` quando precisa.

Isso dá duas coisas. Primeiro, não duplica dado: o staging já é a cópia crua.
Segundo, e mais importante, **reavaliação de graça**. Um item rejeitado hoje
por `produto_inexistente`, porque o cadastro ainda não tinha chegado, é relido
do staging e revalidado na próxima carga — e passa sozinho. Num ERP de
verdade o cadastro vem num job e o movimento vem em outro; dimensão que chega
tarde não pode virar rejeição permanente.

Com staging temporário, a quarentena teria que guardar cópia dos valores, e
reavaliar seria reconstruir um retrato que não existe mais.

### O que exatamente a janela de 7 dias cobre?

A carga incremental não lê "o que mudou desde a última carga". Lê **o que
mudou desde (marca-d'água − 7 dias)**, aplicado sobre `dt_alt`.

A janela existe porque ERP legado grava linha com data de alteração
**retroativa**: importação de arquivo, contingência fiscal, correção manual.
A linha entra no banco hoje carimbada como de 3 dias atrás. Um incremental
ingênuo — `WHERE dt_alt > marca` — nunca a lê. Ela não dá erro; ela
simplesmente não existe no relatório.

A janela de N dias recupera isso até N dias de atraso. **Além de N, não
recupera — e é por isso que a conferência existe.** Linha com 15 dias de
atraso e janela de 7 faz a carga falhar com código 1, apontando o dia. Esse
caso é testado: o CI exige que essa carga **reprove**.

N é configurável. Sete é um palpite calibrado pelo gerador, não uma verdade:
num ERP real o número sai de medir a distribuição de `dt_alt - dt_venda` por
algumas semanas.

### E se eu só aumentar a janela para 90 dias?

Aí todo incremental relê 90 dias de movimento, e deixa de ser incremental. A
janela é um balanço entre custo e alcance; a conferência é o que torna o
balanço seguro, porque falha quando o alcance não bastou.

### O que `--completa` faz que `--incremental` não faz?

`--completa` ignora a marca-d'água, relê a origem inteira e recria a estrela
do zero. `--incremental` lê só a fatia da janela e faz upsert.

Na prática, `--completa` é a saída de emergência: é o que o operador roda
quando a conferência reprova por linha fora da janela. A própria mensagem de
falha diz isso.

As duas terminam na mesma conferência e no mesmo invariante. Não existe modo
"rápido e sem conferir".

### Por que a marca-d'água só avança no fim, e só se tudo passou?

Porque avançar cedo perde dado em silêncio — o único modo de falha que este
desenho recusa.

Se a marca avançasse por etapa, uma carga que morre no meio deixaria a marca
adiantada para as entidades que já passaram. A carga seguinte leria a partir
dali e **nunca** leria o que ficou no meio. Nada daria erro.

Avançando só no fim, carga que morre é carga que não aconteceu: a próxima
relê a mesma fatia. E reler não duplica, porque tudo é
`INSERT ... ON CONFLICT DO UPDATE` por chave natural. A idempotência é o que
torna essa escolha barata.

### Por que a conferência é por dia e loja, e não um total?

Porque um total fecha por coincidência. Dois erros de sinal oposto se anulam,
e o total bate com o dado errado dentro.

Por dia × loja, em contagem **e** em valor, o erro tem endereço: a mensagem de
falha diz quais grupos divergem e quantas linhas faltam. Isso muda a conversa
de "o número está estranho" para "faltam 28 itens no dia 2026-10-27 na loja 1".

Contagem e valor juntos porque qualquer um sozinho tem ponto cego: trocar o
valor de uma linha não muda a contagem, e trocar duas linhas por outras duas
de mesmo total não muda a soma.

### Por que o fato mantém os itens cancelados?

Porque apagar transforma cancelamento em sumiço, e sumiço quebra o invariante.

`fato_venda_item` guarda o cancelado com `cancelado = true` e os valores
originais. A receita filtra `cancelado = false`. Resultado: a conferência
fecha (a linha existe nos dois lados) **e** o cancelamento vira métrica —
`docs/analise.md` mostra R$ 37 mil num mês numa loja só.

O caso que motivou isso é cruel: no ERP, cancelar uma venda atualiza só o
**cabeçalho**. Os itens ficam com status `'N'` e `dt_alt` antigo. Quem extrai
item por data de alteração nunca vê o cancelamento, e a receita fica inflada
para sempre. O conector resolve isso com `reextrair_quando_pai_mudar`: puxa os
itens cujo cabeçalho mudou, mesmo que o item não tenha mudado.

### Por que `fato_estoque_dia` é denso?

Porque ruptura é a **ausência** de venda. Num fato esparso, o dia em que o
produto acabou não tem linha — e não dá para separar "não vendeu porque não
tinha" de "não vendeu porque ninguém quis".

O custo está escrito no README: 720 mil linhas para 4 lojas × 1.500 produtos ×
120 dias, crescendo por dias × lojas × produtos. Não escala para milhões de
pares × anos. Para esse volume, o caminho é guardar só as mudanças de saldo e
reconstruir a densidade na consulta — mais barato em disco, mais caro em cada
`SELECT`, e bem mais difícil de conferir.

### Por que `sistema_origem` em toda chave natural?

Porque dois ERPs usam o mesmo `cod_produto` para produtos diferentes. Sem isso,
integrar o segundo exige trocar as chaves de tudo que já está no DW — e isso
é migração, não integração.

Com `sistema_origem` na chave desde o primeiro dia, o segundo ERP é um YAML
novo e uma carga. O custo foi pagar uma coluna a mais em cada chave antes de
precisar dela.

### Por que o mapeamento está em YAML e não em código?

Porque "integração rápida" e "mexer em `.py` para cada cliente" não cabem na
mesma frase. O YAML define tabela, chave, coluna de alteração e o mapeamento
de colunas; o código não sabe nada sobre o ERP A.

**Onde isso ainda não é verdade, e está escrito:** o vocabulário do ERP (os
status válidos, os tipos de movimento) ainda mora em `tratamento.py`. Chave
composta funciona na extração mas não na quarentena. A marca-d'água assume
`timestamp`, e há ERP que versiona por sequência numérica. Os três só vão ser
corrigidos quando um segundo ERP sintético forçar — ver "Limites conhecidos".

### Por que não dbt?

Restrição do projeto: orçamento zero e dependências mínimas. Mas a resposta
honesta é que dbt resolveria bem a camada tratada → estrela e **não** resolve
a parte difícil daqui: extração incremental com janela sobre `dt_alt`,
re-extração por mudança no pai, e uma quarentena que reavalia.

Num projeto real eu usaria dbt para as transformações e manteria a extração e
a conferência em Python, que é mais ou menos onde este repositório já está —
só que sem a dependência.

### Por que o pipeline lê a origem com um usuário só de `SELECT`?

Porque o ERP é o sistema que paga a conta do cliente, e um pipeline de BI não
tem motivo nenhum para escrever nele. `erp_ro` tem `SELECT` e nada mais;
`erp_app` (dono da origem) só é usado pelo gerador, que é o "ERP" sintético.

É a resposta curta para a pergunta que todo gerente de TI de supermercado faz
antes de liberar acesso: "o que exatamente isso faz no meu ERP?". Nada. Lê.

### O que acontece se a carga morrer no meio?

Nada é perdido e nada é duplicado. A marca-d'água não avançou, então a próxima
carga relê a mesma fatia; todo escrita é upsert por chave natural, então reler
reescreve a mesma linha. A etapa que falhou fica registrada em `ctl.etapa` com
sufixo `:FALHA`, e o erro original sobe — não é mascarado pelo erro de
registrar o erro.

### Como você integraria um ERP novo?

Levantar as tabelas, as chaves naturais e a coluna de alteração de cada uma;
escrever `config/conectores/<erp>.yaml`; rodar `--completa`; conferir os
totais por dia e loja contra um relatório do próprio ERP; ligar o
`--incremental`; só então agendar.

A ordem importa: a conferência contra o relatório do cliente vem **antes** de
ligar o incremental. É o passo que transforma "o pipeline rodou" em "o número
está certo".

---

## Sobre como este repositório foi escrito

Este projeto foi escrito em par com um assistente de IA (Claude Code), e os
commits registram isso. O desenho, as restrições e as decisões são minhas —
estão em `CLAUDE.md`, escrito antes da primeira linha de código, e cada uma
delas está defendida acima.

O que o par rendeu, e o que não rendeu, está em
[`construcao.md`](construcao.md): a caçada de performance que derrubou a carga
completa de 127 s para 64 s depois de **duas hipóteses erradas** testadas e
descartadas, uma asserção de teste minha que estava errada, um furo no dado
sintético e uma suíte de testes que travou o banco.

Prefiro declarar isso a ser perguntado. O que vale avaliar num projeto de
engenharia de dados não é quem digitou — é se o invariante está certo, se os
números foram medidos e se os limites estão escritos. Os três estão.
