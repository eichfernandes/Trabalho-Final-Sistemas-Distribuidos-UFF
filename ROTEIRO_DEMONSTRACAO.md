# Roteiro de demonstração (≈ 12 minutos)

## Antes de apresentar (faça 10 minutos antes)

1. Docker Desktop aberto, com pelo menos 6 GB de memória.
2. Ambiente zerado e no ar:
   ```bash
   docker compose down -v
   docker compose up -d
   docker compose up -d streaming
   ```
3. Modelo e replay existem (`model/modelo_rf.joblib` e `data/reservas_replay.csv`); se não, `docker compose run --rm treino`.
4. Confira que o job está pronto: `docker compose logs --tail 5 streaming` mostra `Streaming ativo`.
5. Deixe abertos: o painel (`streamlit run consumer/painel.py` → <http://localhost:8501>), a interface do Spark (<http://localhost:4040>) e o cluster (<http://localhost:8080>).
6. **Rode o teste de carga uma vez antes** (`python scripts/teste_carga.py`) e guarde o resultado: ele leva alguns minutos e é melhor mostrar o número já pronto. Depois zere o ambiente de novo (passo 2).

---

## 1. O problema e a ideia (1 min)

- Cerca de 37% das reservas da base *Hotel Booking Demand* terminam em cancelamento ou no-show: quarto vazio e receita perdida.
- Hoje o hotel só descobre quando já é tarde. A ideia: tratar cada reserva que chega como um **evento** e prever na hora a probabilidade de o hóspede chegar.
- Faixas de ação: abaixo de 40% risco alto; de 40% a 60% vale ligar ou fazer uma oferta; acima de 60% preservar a reserva.

## 2. Arquitetura (2 min)

```
Producer → Kafka (reservas) → Spark Streaming + Random Forest → Kafka (previsoes) → Painel
```

Pontos para falar:
- **Kafka** desacopla quem produz de quem processa e guarda as mensagens em disco (permite reprocessar). O tópico tem **3 partições**: a carga é dividida entre elas.
- **Spark Structured Streaming** lê em micro-lotes (2 s), limpa os dados e aplica o Random Forest **em paralelo nos executors** com pandas UDF. Cada partição do Kafka vira uma tarefa; há 2 workers com 2 cores.
- O modelo é treinado **offline** com o histórico, e só a inferência roda em streaming. O treino usa 80% dos dados; o replay usa só os **20% de teste**, que o modelo nunca viu, então a acurácia mostrada é honesta.
- Mostre rapidamente <http://localhost:8080>: 2 workers `ALIVE`.

## 3. Demonstração ao vivo (4 min)

Inicie o producer: `python producer/producer.py --taxa 50`

No painel:
1. **Topo:** os contadores sobem; mostre a divisão por faixa de risco e a **receita em risco**.
2. **Visão geral:** leia um insight automático (por exemplo, o tipo de depósito mais arriscado). Use o seletor "Agrupar por".
3. **Alertas:** reservas de risco alto e médio, da mais crítica para a menos. É o que a equipe do hotel usaria para ligar ou fazer uma oferta.
4. **Qualidade do modelo:** a taxa real de comparecimento cresce de risco alto para risco baixo. Explique que o resultado real só existe porque os dados são simulados.
5. **Sistema distribuído:** vazão, **latência de ponta a ponta** (segundos) e a distribuição pelas 3 partições.
6. Em <http://localhost:4040> (aba *Structured Streaming* e *Executors*): os micro-lotes e as tarefas distribuídas entre os workers.

## 4. Tolerância a falhas (3 min)

Com o producer ainda enviando (ou rodando `python scripts/teste_resiliencia.py`, que faz tudo sozinho):

1. `docker compose kill -s SIGKILL streaming` — **derruba o Spark de repente**. No painel, a vazão cai a zero.
2. O producer continua; as reservas **se acumulam no Kafka** (o desacoplamento em ação).
3. `docker compose start streaming` — o Spark volta, lê o **checkpoint** e processa o que ficou pendente. A vazão dispara.
4. `python scripts/verificar_consistencia.py` — **nenhuma reserva perdida**.

Seja direto sobre a garantia: o checkpoint evita perda, mas a escrita no Kafka é **pelo menos uma vez**, então o lote em andamento na queda pode ser reenviado. Por isso o painel é idempotente (descarta repetidas pelo `id_reserva`) e avisa quantas ignorou.

## 5. Reprocessamento (1 min, opcional)

Mostre que o Kafka guardou as reservas: apague o checkpoint e o tópico `previsoes` (README, seção 12.5), reinicie o `streaming` e veja todas as previsões serem recalculadas a partir do `reservas`. Útil, por exemplo, para aplicar um modelo novo ao histórico.

## 6. Desempenho (1 min)

Mostre o resultado do teste de carga já coletado: vazão e latência por taxa de chegada. Explique o **ponto de saturação**: acima da capacidade (`max-por-lote ÷ intervalo`, 500 reservas/s na configuração padrão) as reservas se acumulam no Kafka e a latência cresce, e dá para aumentar a capacidade ajustando o lote, o intervalo ou o número de workers e partições.

---

## Perguntas que o professor pode fazer

**Por que o Kafka tem partições?** Para dividir a carga: cada partição pode ser lida em paralelo por uma tarefa do Spark. A chave da mensagem é o id da reserva, que distribui as mensagens uniformemente.

**O que acontece se o Kafka cair?** Temos um único broker (fator de replicação 1), então ele é um ponto único de falha neste projeto. Em produção seriam vários brokers com replicação. O que testamos foi a queda do processamento (Spark).

**Por que "pelo menos uma vez" e não "exatamente uma vez"?** O sink Kafka do Spark Structured Streaming oferece pelo menos uma vez; para exatamente uma vez, o consumidor precisa ser idempotente, que é o que o painel faz.

**Por que o modelo não é treinado em streaming?** Um Random Forest não aprende de forma incremental. O treino é offline; no streaming só se faz a inferência. Um retreino periódico com os dados novos seria a evolução natural.

**Como escalar?** Mais partições no tópico, mais workers e executors no Spark, e lotes maiores. O modelo é carregado uma vez por executor, então a inferência escala com o número de cores.

**Os dados são reais?** O dataset é real (*Hotel Booking Demand*), mas o fluxo é simulado: o producer reproduz as reservas em ordem cronológica de reserva.

## Limitações a admitir (honestidade conta ponto)

- Um único broker Kafka e um único master Spark (sem alta disponibilidade).
- Modelo estático: não se atualiza sozinho com novos dados.
- Os dados simulados vêm do mesmo dataset do treino (a parte de teste), então o desempenho em reservas realmente novas pode ser diferente.
- A latência medida inclui o intervalo de micro-lote (2 s por padrão): é um sistema de *near real-time*, não de milissegundos.
