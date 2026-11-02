# Trabalho Final — Sistemas Distribuídos

Previsão em tempo real da probabilidade de uma reserva de hotel se confirmar (base Hotel Booking Demand), usando **Kafka** e **Spark Structured Streaming**.

Fluxo: `Producer → Kafka (reservas) → Spark Streaming → Kafka (previsoes) → Consumidor`

> **Status:** em desenvolvimento. Este README é atualizado a cada etapa.
> Etapas concluídas até agora: **1 — Infraestrutura**, **2 — Treino do modelo**, **3 — Producer**, **4 — Spark Streaming**, **5 — Painel** e **6 — Testes de resiliência e desempenho**.

---

## Início rápido (com tudo já instalado)

Depois de seguir as seções 2 a 10 uma vez, a demonstração completa usa **3 terminais**, todos na raiz do projeto (nos que usam Python, com o ambiente virtual ativo):

```bash
# Terminal 1 - infraestrutura e job de streaming
docker compose up -d
docker compose up -d streaming

# Terminal 2 - painel (abra http://localhost:8501 no navegador)
streamlit run consumer/painel.py

# Terminal 3 - simula a chegada das reservas
python producer/producer.py --taxa 50
```

Para parar: `Ctrl+C` no painel e no producer, e `docker compose down` para desligar o Docker. Para recomeçar do zero, use `docker compose down -v`.

---

## 1. Pré-requisitos

| Item | Mínimo recomendado |
|---|---|
| Memória RAM | 8 GB (o cluster usa ~3 GB; no Docker Desktop, configure pelo menos 6 GB — seção 2) |
| Espaço em disco | 8 GB livres (as imagens Docker somam ~3 GB) |
| Internet | necessária na primeira execução (download das imagens Docker) |
| Docker + Docker Compose | instalação abaixo |
| Python 3.10 ou superior | para o producer e o consumer (seção 7) |

O Docker Compose já vem junto com o Docker Desktop (Windows/macOS) e como plugin no Docker Engine (Linux). Todos os comandos usam a sintaxe moderna `docker compose` (com espaço).

---

## 2. Instalando o Docker

### Windows 10/11

1. Abra o **PowerShell como administrador** e instale o WSL 2:
   ```powershell
   wsl --install
   ```
   Reinicie o computador quando solicitado.
2. Baixe e instale o **Docker Desktop** em <https://www.docker.com/products/docker-desktop/>.
3. Na instalação, mantenha marcada a opção **"Use WSL 2 instead of Hyper-V"**.
4. Abra o Docker Desktop e aguarde o ícone da baleia indicar *"Docker Desktop is running"*.
5. (Recomendado) Em *Settings → Resources*, garanta pelo menos **4 GB de memória** para o Docker. Se usar WSL 2, isso é configurado no arquivo `%UserProfile%\.wslconfig`:
   ```ini
   [wsl2]
   memory=6GB
   ```
   Depois execute `wsl --shutdown` e reabra o Docker Desktop.

### macOS

1. Baixe o **Docker Desktop** em <https://www.docker.com/products/docker-desktop/> (escolha a versão *Apple Silicon* ou *Intel* conforme o seu Mac).
2. Arraste para *Applications*, abra e aceite os termos.
3. Em *Settings → Resources*, garanta pelo menos **4 GB de memória**.

### Linux (Ubuntu/Debian)

```bash
# Remove versões antigas (se existirem)
sudo apt-get remove -y docker docker-engine docker.io containerd runc

# Dependências e chave oficial do Docker
sudo apt-get update
sudo apt-get install -y ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Repositório oficial (em Debian, troque "ubuntu" por "debian" nas duas linhas)
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
| sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Instala Docker Engine + plugin do Compose
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Permite usar docker sem sudo (faça logout/login depois)
sudo usermod -aG docker $USER
```

Para outras distribuições, siga <https://docs.docker.com/engine/install/>.

### Conferindo a instalação

```bash
docker --version
docker compose version
docker run --rm hello-world
```

Se os três comandos funcionarem, o Docker está pronto.

---

## 3. Obtendo o projeto

Com Git:

```bash
git clone <URL-DO-REPOSITORIO> trabalho_final_sd
cd trabalho_final_sd
```

Ou baixe o `.zip` do projeto, extraia e entre na pasta `trabalho_final_sd`.

Todos os comandos a seguir devem ser executados **dentro dessa pasta** (onde está o `docker-compose.yml`).

---

## 4. Subindo a infraestrutura (Etapa 1)

```bash
docker compose up -d --build
```

Na primeira vez, o Docker baixa a imagem do Kafka e **constrói a imagem do Spark** (Python, Java e as bibliotecas de ML), o que leva de 5 a 10 minutos. Nas próximas vezes basta `docker compose up -d`. O comando sobe:

| Serviço | Função |
|---|---|
| `kafka` | Broker Kafka (modo KRaft, sem Zookeeper) |
| `kafka-init` | Cria os tópicos `reservas` e `previsoes` (3 partições cada) e encerra |
| `spark-master` | Coordenador do cluster Spark (imagem própria, `spark/Dockerfile`) |
| `spark-worker-1`, `spark-worker-2` | Executores (2 cores e 1 GB cada) |

### Portas utilizadas

| Porta | Uso |
|---|---|
| `29092` | Kafka — acesso a partir da sua máquina (producer/consumer) |
| `8080` | Interface web do Spark Master → <http://localhost:8080> |
| `8081` / `8082` | Interface web dos workers 1 e 2 |
| `7077` | Endereço do Spark Master |
| `4040` | Interface do job de streaming (quando o `streaming` está rodando) |
| `8501` | Painel Streamlit (Etapa 5) |

Se alguma porta já estiver em uso na sua máquina, encerre o programa que a ocupa ou altere o lado esquerdo do mapeamento no `docker-compose.yml` (ex.: `"8090:8080"`).

---

## 5. Verificando se está tudo funcionando

**5.1. Status dos containers**

```bash
docker compose ps -a
```

Esperado: `kafka` como `healthy`; `spark-master`, `spark-worker-1` e `spark-worker-2` como `running`; `kafka-init` como `exited (0)` (é o comportamento correto, ele só cria os tópicos).

**5.2. Tópicos criados**

```bash
docker compose logs kafka-init
```

Deve terminar listando `previsoes` e `reservas`.

**5.3. Cluster Spark**

Abra <http://localhost:8080>. A página deve mostrar **2 workers** com status `ALIVE`.

**5.4. Teste de ida e volta no Kafka**

Cria um tópico temporário, publica uma mensagem, lê de volta e apaga o tópico:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic teste --partitions 1 --replication-factor 1

echo "ola kafka" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic teste

docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic teste --from-beginning --max-messages 1 --timeout-ms 15000

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic teste
```

O terceiro comando deve imprimir `ola kafka`.

---

## 6. Comandos do dia a dia

| Ação | Comando |
|---|---|
| Ver logs de um serviço | `docker compose logs -f kafka` |
| Parar tudo (mantém os dados do Kafka) | `docker compose down` |
| Subir novamente | `docker compose up -d` |
| **Resetar tudo** (apaga mensagens e tópicos) | `docker compose down -v` |
| **Zerar o pipeline inteiro** (mensagens, tópicos e checkpoint do Spark) | `docker compose down -v` e depois `docker compose up -d` |
| Listar tópicos | `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list` |
| Ver detalhes das partições | `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic reservas` |

---

## 7. Preparando o ambiente Python (producer e consumer)

O producer (Etapa 3) e o consumer (Etapa 5) rodam na **sua máquina**, fora do Docker, então é preciso ter o Python. O treino do modelo e o Spark rodam dentro do Docker (seções 8 e 10) e não dependem do seu Python.

### 7.1. Instalando o Python

- **Windows:** baixe o instalador em <https://www.python.org/downloads/> (versão 3.10 ou superior). **Na primeira tela, marque a caixa "Add python.exe to PATH"** antes de clicar em *Install Now*.
- **macOS:** `brew install python` (com Homebrew) ou o instalador de python.org.
- **Linux (Ubuntu/Debian):** `sudo apt-get install -y python3 python3-venv python3-pip`

Confira (em um terminal **novo**, aberto depois da instalação):

```bash
python --version      # Windows (se não funcionar, tente: py --version)
python3 --version     # macOS / Linux
```

### 7.2. Criando o ambiente virtual e instalando as dependências

Na pasta do projeto:

**Windows (PowerShell)**
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Se o PowerShell bloquear a ativação com um erro sobre *execução de scripts desabilitada*, rode uma vez `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`, responda `S` e repita a ativação.

**macOS / Linux**
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Quando o ambiente está ativo, o terminal mostra `(.venv)` no início da linha. **Sempre ative o ambiente antes de rodar os scripts do projeto** (em cada terminal novo).

---

## 8. Treinando o modelo (Etapa 2)

O treino é executado **uma única vez**, **dentro do Docker**, na mesma imagem usada pelos executors do Spark. Isso é importante: o arquivo do modelo (`.joblib`) só é confiável se o treino e a inferência usarem exatamente as mesmas versões de Python, pandas e scikit-learn.

Com o Docker rodando, na raiz do projeto:

```bash
docker compose build spark-master
docker compose run --rm treino
```

O primeiro comando constrói a imagem (só demora na primeira vez; se você já fez `docker compose up -d --build`, termina em segundos). O segundo baixa o dataset *Hotel Booking Demand* do Kaggle (precisa de internet; se o arquivo `data/hotel_bookings.csv` já existir, ele é reaproveitado), limpa os dados, treina o Random Forest e avalia. Leva de 1 a 3 minutos.

> **Se você já treinou o modelo antes pelo Python local, rode este passo de novo.** O modelo antigo foi gerado com outras versões de biblioteca e não deve ser usado pelo Spark.

**Se o download automático falhar:** baixe o arquivo manualmente em <https://www.kaggle.com/datasets/jessemostipak/hotel-booking-demand>, extraia o `hotel_bookings.csv` para a pasta `data/` e rode o comando novamente.

### O que ele gera

| Arquivo | Conteúdo |
|---|---|
| `model/modelo_rf.joblib` | Pipeline treinado (one-hot encoding + Random Forest) |
| `model/metadados.json` | Parâmetros usados no treino (teto de ADR, países mantidos, limiares, versões das bibliotecas, métricas) |
| `data/reservas_replay.csv` | Reservas que o producer vai reproduzir, em ordem cronológica de reserva |

Esses arquivos **não vão para o GitHub** (estão no `.gitignore`): quem clonar o projeto os recria com o comando acima.

### Como conferir se deu certo

A saída no terminal deve terminar com `Concluido.` e mostrar, na seção de avaliação, valores na faixa de:

- ROC-AUC em torno de **0,92 a 0,95**
- Acurácia em torno de **0,85 a 0,88**
- Tamanho do modelo de algumas dezenas de MB (se passar de ~150 MB, avise)
- Uma distribuição nas três faixas de risco (< 40%, 40–60%, > 60%)

Confira também que existem os três arquivos da tabela acima:

```bash
dir model data        # Windows
ls model data         # macOS / Linux
```

### Decisões de projeto desta etapa

- O replay usa **somente os 20% de teste**, que o modelo nunca viu. Assim a acurácia mostrada em tempo real no painel é honesta.
- O pré-processamento está em `common/preprocessing.py`, usado tanto no treino quanto no Spark, para garantir que o modelo veja as mesmas features nos dois lugares.
- O resultado real da reserva (`desfecho_real`) viaja no evento **apenas** para o painel comparar previsão com realidade. Ele nunca é usado como entrada do modelo.

---

## 9. Executando o producer (Etapa 3)

O producer lê o `data/reservas_replay.csv` e publica cada reserva, em ordem cronológica, como uma mensagem JSON no tópico `reservas` do Kafka.

### 9.1. Antes de rodar

1. O Kafka precisa estar no ar: `docker compose up -d` e `docker compose ps -a` (o `kafka` deve estar `healthy`).
2. O treino da Etapa 2 precisa ter sido feito (o arquivo `data/reservas_replay.csv` deve existir).
3. O ambiente virtual deve estar ativo e com a dependência nova instalada (o `requirements.txt` ganhou o `confluent-kafka`):
   ```bash
   pip install -r requirements.txt
   ```

### 9.2. Teste rápido (100 reservas)

```bash
python producer/producer.py --limite 100
```

Deve imprimir a conexão com o Kafka, o progresso e, no final, um resumo parecido com:

```
Enviadas: 100 | Entregues e confirmadas pelo Kafka: 100 | Erros: 0
Mensagens por partição: {0: 34, 1: 31, 2: 35}
```

Conferência: `Enviadas` e `Entregues` devem ser iguais, `Erros` deve ser 0, e as mensagens devem aparecer **nas 3 partições** (é o Kafka dividindo a carga).

### 9.3. Conferindo o que chegou no Kafka

Ler as 3 primeiras mensagens do tópico:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic reservas --from-beginning --max-messages 3
```

Cada linha é uma reserva em JSON (campos como `id_reserva`, `hotel`, `lead_time`, `deposit_type`, `data_reserva`, `desfecho_real`, `ts_envio_ms`).

Ver quantas mensagens há em cada partição:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic reservas
```

A saída tem o formato `reservas:partição:total`. A soma dos três totais é o número de mensagens no tópico.

### 9.4. Opções do producer

| Opção | Efeito | Padrão |
|---|---|---|
| `--taxa N` | N reservas por segundo (`0` = velocidade máxima) | 5 |
| `--limite N` | envia só as N primeiras reservas | todas |
| `--csv CAMINHO` | usa outro arquivo de replay | `data/reservas_replay.csv` |
| `--topico NOME` | tópico de destino | `reservas` |
| `--bootstrap HOST:PORTA` | endereço do Kafka | `localhost:29092` |

O replay completo tem cerca de 17 mil reservas. A 5 reservas/s isso leva quase 1 hora; para uma demonstração use algo como `--taxa 50` (~6 minutos).

### 9.5. Atenção: rodar duas vezes duplica as mensagens

O Kafka guarda as mensagens em disco. Se o producer for executado de novo, as reservas são publicadas outra vez. Para começar do zero, use `docker compose down -v` seguido de `docker compose up -d`. Isso apaga todas as mensagens, os tópicos e o checkpoint do Spark, e recria os tópicos vazios.

---

## 10. Executando o Spark Streaming (Etapa 4)

O job lê o tópico `reservas`, descarta mensagens corrompidas e reservas inválidas, aplica o Random Forest nos executors (pandas UDF), classifica a faixa de risco e publica o resultado no tópico `previsoes`. Ele roda dentro do Docker, no serviço `streaming`.

### 10.1. Antes de rodar

1. Infraestrutura no ar e imagem construída: `docker compose up -d --build` (seção 4).
2. Modelo treinado **dentro do Docker**: `docker compose run --rm treino` (seção 8).
3. Tópicos vazios (sem sobras de testes anteriores). Em caso de dúvida: `docker compose down -v` seguido de `docker compose up -d` zera tudo (depois repita o passo 2 se a pasta `model/` tiver sido limpa; ela não é apagada pelo `down -v`).

### 10.2. Iniciando o job

```bash
docker compose up -d streaming
docker compose logs -f streaming
```

Na **primeira vez** o Spark baixa o conector do Kafka (~1 minuto, precisa de internet; depois fica em cache). O job está pronto quando aparecer:

```
Streaming ativo: 'reservas' -> 'previsoes' (micro-lotes a cada 2s, ate 1000 reservas por lote).
```

Para sair dos logs sem parar o job, use `Ctrl+C` (o job continua rodando em segundo plano).

### 10.3. Gerando reservas e vendo as previsões

Em **outro terminal** (com o ambiente virtual ativo), publique reservas:

```bash
python producer/producer.py --limite 200 --taxa 20
```

Nos logs do `streaming` devem aparecer linhas como:

```
lote    3 |    40 reservas |   310.5 reservas/s
```

Leia algumas previsões do tópico de saída:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic previsoes --from-beginning --max-messages 3
```

Cada mensagem é um JSON com `id_reserva`, `probabilidade` (de o hóspede chegar), `faixa_risco` (`alto` < 40%, `medio` 40–60%, `baixo` > 60%), `desfecho_real` e carimbos de tempo para medir latência. Conferência de quantidade:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic previsoes
```

A soma dos totais das partições deve ser igual ao número de reservas enviadas (as reservas do replay já são válidas, então nenhuma é descartada).

### 10.4. Interfaces para acompanhar

| Endereço | O que mostra |
|---|---|
| <http://localhost:8080> | Cluster: workers, aplicação `previsao-reservas` em execução, cores e memória em uso |
| <http://localhost:4040> | Job de streaming: aba *Structured Streaming* (vazão por lote) e *Executors* (as tarefas distribuídas entre os 2 workers) |

### 10.5. Parando e retomando (checkpoint)

```bash
docker compose stop streaming     # para o job
docker compose start streaming    # retoma de onde parou
```

O Spark guarda no *checkpoint* até onde já leu no Kafka. Ao reiniciar, ele continua dali, **sem perder reservas**. A garantia de escrita no Kafka é *pelo menos uma vez*: o lote que estava em andamento na queda pode ser escrito de novo, gerando algumas previsões repetidas (o painel as ignora). Para testar: com o producer enviando, pare o job com `stop`, espere alguns segundos, rode `start` e depois confira com `python scripts/verificar_consistencia.py` (seção 12) que nenhuma reserva ficou sem previsão.

### 10.6. Zerando para recomeçar

O checkpoint fica num volume do Docker e **precisa ser apagado junto com os tópicos**: se você apagar só o tópico `reservas`, o job tentará continuar de offsets que não existem mais e vai falhar. O jeito mais simples de zerar tudo:

```bash
docker compose down -v
docker compose up -d
```

(Isso apaga também o cache do conector Kafka, que será baixado de novo no próximo `streaming`.)

---

## 11. Executando o painel (Etapa 5)

O painel é uma página web (Streamlit) que lê o tópico `previsoes` do Kafka e se atualiza sozinha a cada 2 segundos. Ele roda na **sua máquina**, como o producer.

### 11.1. Preparação (uma vez)

1. Instale a dependência nova (o `requirements.txt` ganhou o `streamlit`), com o ambiente virtual ativo:
   ```bash
   pip install -r requirements.txt
   ```
2. O job de streaming ganhou o campo `noites` na saída (usado para calcular a receita em risco). Reinicie-o para carregar a versão nova:
   ```bash
   docker compose restart streaming
   ```
   Para uma demonstração limpa, o mais simples é zerar tudo: `docker compose down -v`, depois `docker compose up -d` e `docker compose up -d streaming`. Previsões geradas antes dessa mudança não têm `noites` e ficam fora do cálculo de receita.

### 11.2. Abrindo o painel

```bash
streamlit run consumer/painel.py
```

Abra <http://localhost:8501> no navegador. (O navegador não abre sozinho; se o Windows perguntar sobre o firewall, permita o acesso.)

O painel começa vazio, com a mensagem "Aguardando previsões", e se preenche quando as reservas chegam.

### 11.3. Gerando reservas para ver o painel funcionando

Em outro terminal, com o ambiente virtual ativo:

```bash
python producer/producer.py --taxa 50
```

São cerca de 17 mil reservas; a 50 por segundo leva uns 6 minutos. Deixe o painel aberto durante o envio para ver os números subirem e a latência ser medida.

### 11.4. O que cada seção mostra

| Seção | Conteúdo |
|---|---|
| **Topo (sempre visível)** | Reservas processadas, contagem por faixa de risco e receita estimada em risco alto (diária × noites, em euros) |
| **Visão geral** | Insights automáticos, reservas por faixa, distribuição das probabilidades, risco por tipo de depósito / segmento / hotel / cliente e efeito da antecedência da reserva |
| **Alertas** | Tabela das reservas de risco alto e médio, da mais crítica para a menos (também por receita ou por chegada), com o que a equipe precisa para agir |
| **Qualidade do modelo** | Compara a previsão com o resultado real: acurácia, taxa real de comparecimento por faixa e matriz de confusão |
| **Sistema distribuído** | Vazão, latência de ponta a ponta, tempo no Spark e distribuição das mensagens pelas partições do Kafka |

### 11.5. Como o painel funciona

- Uma thread de fundo consome o tópico `previsoes` e guarda as previsões em memória. Ela lê **desde o começo do tópico** a cada início, sem confirmar offsets: o Kafka guarda as mensagens em disco, então abrir o painel depois reconstrói todo o histórico (reprocessamento).
- O botão **Recarregar do início** (barra lateral) descarta a tela e lê o tópico de novo.
- A latência de ponta a ponta só é medida para reservas que chegam **enquanto o painel está aberto**. Ela compara o instante do envio pelo producer com o da chegada aqui; ambos usam o relógio da sua máquina.

---

## 12. Testes de resiliência e desempenho (Etapa 6)

Esta etapa reúne as evidências de sistemas distribuídos: tolerância a falhas (checkpoint), reprocessamento (o Kafka guarda as mensagens), vazão e latência. Tudo roda na sua máquina, com o Docker e o job `streaming` no ar. Use **tópicos zerados** para os números ficarem limpos: `docker compose down -v`, depois `docker compose up -d` e `docker compose up -d streaming`.

### 12.1. Garantias de entrega: o que o pipeline promete

| Etapa do fluxo | Garantia |
|---|---|
| Producer → Kafka | Confirmação de cada mensagem e envio idempotente (sem duplicar por reenvio de rede) |
| Kafka → Spark | O checkpoint guarda até onde o Spark leu: após uma queda, ele continua dali e **nenhuma reserva é perdida** |
| Spark → Kafka (`previsoes`) | **Pelo menos uma vez**: se o job cair no meio de um lote, esse lote pode ser escrito de novo, gerando previsões repetidas |
| Painel | Idempotente: se a mesma reserva aparece duas vezes, vale a última previsão e o painel avisa quantas ignorou |

### 12.2. Verificar a consistência (a qualquer momento)

```bash
python scripts/verificar_consistencia.py
```

Lê os tópicos `reservas` e `previsoes` inteiros e informa quantas reservas **ficaram sem previsão** (perda, que não deve acontecer) e quantas previsões estão **repetidas**. Retorna sucesso se nenhuma reserva foi perdida. Se o job ainda estiver processando, espere alguns segundos e rode de novo.

### 12.3. Teste de falha: derrubar o Spark no meio

```bash
python scripts/teste_resiliencia.py
```

O script envia 3000 reservas (50 por segundo) e, após 20 segundos, **mata o container do Spark** (`SIGKILL`, uma queda abrupta). Mantém o job fora do ar por 15 segundos, enquanto as reservas continuam chegando ao Kafka, e então o religa. O Spark retoma do checkpoint, processa o que ficou acumulado e o script mostra o relatório de consistência.

Resultado esperado: **reservas sem previsão: 0**. Pode haver algumas previsões repetidas (no máximo o lote que estava em andamento na queda, cerca de uma centena); o painel as ignora. Opções: `--reservas`, `--taxa`, `--derrubar-apos`, `--pausa`.

Para mostrar ao vivo, abra o painel durante o teste: a vazão cai a zero quando o Spark morre e dispara quando ele volta.

### 12.4. Teste de carga: vazão e latência

```bash
python scripts/teste_carga.py
```

Testa taxas crescentes de chegada (20, 100, 300 e 600 reservas por segundo, durante 20 s cada) e mede a vazão de saída e a latência de ponta a ponta (p50, p95, p99), salvando em `data/resultados_carga.csv`. Opções: `--taxas 50 200 400 800`, `--duracao 30`.

Como ler o resultado: enquanto a taxa de chegada está abaixo da capacidade do Spark, a latência fica em poucos segundos. Quando passa da capacidade, as reservas se acumulam no Kafka e a latência cresce: é o **ponto de saturação**. Com a configuração padrão, a capacidade teórica é `max-por-lote ÷ intervalo` = 1000 ÷ 2 s = **500 reservas/s**, então a rodada de 600/s deve aparecer como saturada.

**Aumentando a capacidade:** o tamanho do lote e o intervalo são configuráveis. Por exemplo, para até 4000 reservas por lote a cada 1 segundo (PowerShell):

```powershell
$env:MAX_POR_LOTE=4000; $env:INTERVALO=1; docker compose up -d streaming
python scripts/teste_carga.py --taxas 300 600 1000
```

(Para voltar ao padrão: `Remove-Item Env:MAX_POR_LOTE, Env:INTERVALO` e `docker compose up -d streaming`.) O limite agora passa a ser o hardware: os 2 workers têm 2 cores cada, e o modelo roda em Python.

### 12.5. Reprocessamento: o Kafka guarda as mensagens

O Kafka mantém as reservas em disco, então é possível apagar todos os resultados e recalculá-los. Com o `streaming` já tendo processado reservas:

```powershell
docker compose stop streaming
docker compose rm -f streaming
docker volume rm trabalho_final_sd_checkpoints
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic previsoes
docker compose up kafka-init
docker compose up -d streaming
```

O que isso faz: apaga o checkpoint e o tópico de resultados (`previsoes`), mas **não** o de entrada (`reservas`). Ao iniciar sem checkpoint, o Spark lê o `reservas` desde o começo e gera todas as previsões de novo (por exemplo, com um modelo atualizado). Quando terminar, confira com `python scripts/verificar_consistencia.py`: reservas e previsões devem coincidir. O painel (botão **Recarregar do início**) mostra tudo de novo.

### 12.6. Resumo: o que cada teste demonstra

| Conceito de sistemas distribuídos | Onde aparece |
|---|---|
| Desacoplamento produtor/consumidor | Producer e Spark independentes; o Spark pode cair sem o producer parar (12.3) |
| Persistência e reprocessamento | O Kafka guarda as reservas em disco (12.5) |
| Tolerância a falhas | Checkpoint do Spark (12.3) |
| Semântica de entrega | Pelo menos uma vez no sink + consumidor idempotente (12.1) |
| Paralelismo | Partições do Kafka → tarefas do Spark em 2 workers (painel e <http://localhost:4040>) |
| Escalabilidade e saturação | Teste de carga e ajuste do lote (12.4) |

---

## 13. Problemas comuns

**`Cannot connect to the Docker daemon`** — o Docker não está rodando. Abra o Docker Desktop (Windows/macOS) ou execute `sudo systemctl start docker` (Linux).

**`permission denied` ao rodar `docker` no Linux** — você ainda não foi adicionado ao grupo `docker`. Execute `sudo usermod -aG docker $USER` e faça logout/login (ou use `sudo`).

**`port is already allocated`** — outra aplicação usa a porta. Veja a seção de portas acima.

**`kafka` fica `unhealthy` ou reinicia** — normalmente falta de memória. Aumente a memória do Docker (seção 2) e rode `docker compose down -v && docker compose up -d`.

**Workers não aparecem em `localhost:8080`** — aguarde ~20 segundos e atualize a página; se persistir, `docker compose logs spark-worker-1`.

**Falha ao baixar imagens** — verifique a conexão com a internet e se o Docker consegue acessar o Docker Hub (`docker pull hello-world`).

**`python` não é reconhecido (Windows)** — o Python não foi adicionado ao PATH. Reinstale marcando "Add python.exe to PATH", ou use `py` no lugar de `python`. Abra um terminal novo depois de instalar.

**`pip install` falha ao compilar algum pacote** — normalmente é uma versão do Python muito nova ou muito antiga. Use Python entre 3.10 e 3.13.

**`ModuleNotFoundError: No module named 'sklearn'` (ou pandas, joblib...)** — o ambiente virtual não está ativo. Ative-o (seção 7.2) e rode de novo.

**`FileNotFoundError` / dataset não encontrado** — veja a seção 8 sobre o download manual do dataset.

**`Nao foi possivel conectar ao Kafka em localhost:29092`** — o Docker não está rodando ou os containers estão desligados. Rode `docker compose up -d` e confira com `docker compose ps -a`.

**`O topico 'reservas' nao existe`** — o `kafka-init` não rodou. Execute `docker compose up kafka-init` e veja `docker compose logs kafka-init`.

**`ModuleNotFoundError: No module named 'confluent_kafka'`** — falta instalar a dependência nova: `pip install -r requirements.txt` com o ambiente virtual ativo.

**`Arquivo nao encontrado: ...reservas_replay.csv`** — o treino ainda não foi executado (seção 8).

**Falha no `docker compose build` (apt ou pip)** — normalmente é rede. Tente de novo; se persistir, verifique proxy/VPN e se o Docker consegue baixar da internet.

**`Modelo nao encontrado em model/`** (log do `streaming`) — falta rodar `docker compose run --rm treino`.

**`Failed to find data source: kafka`** ou erro de download do pacote — o conector Kafka não foi baixado (sem internet no primeiro uso). Confira a conexão e rode `docker compose up -d streaming` de novo.

**`Initial job has not accepted any resources`** — os workers não estão disponíveis ou faltam recursos. Confira em <http://localhost:8080> se há 2 workers `ALIVE` e se o Docker tem pelo menos 6 GB de memória.

**Erro de offsets / `Partition reservas-0's offset was changed`** — o tópico foi recriado mas o checkpoint antigo ficou. Zere tudo: `docker compose down -v` e `docker compose up -d`.

**O `streaming` encerra logo após subir** — veja o motivo com `docker compose logs streaming`.

**`streamlit` não é reconhecido** — o ambiente virtual não está ativo ou falta `pip install -r requirements.txt`. Alternativa: `python -m streamlit run consumer/painel.py`.

**Painel mostra "Sem conexão com o Kafka"** — o Docker não está rodando ou o Kafka está desligado. Rode `docker compose up -d` e confira `docker compose ps -a`. O painel tenta reconectar sozinho.

**Painel fica em "Aguardando previsões"** — conectou, mas o tópico `previsoes` está vazio. Confira se o `streaming` está rodando (`docker compose logs --tail 20 streaming`) e se o producer foi iniciado.

**A latência de ponta a ponta aparece como "—"** — nenhuma reserva chegou ao vivo ainda. Inicie o producer com o painel aberto.

**Porta 8501 em uso** — rode em outra porta: `streamlit run consumer/painel.py --server.port 8502`.

**`verificar_consistencia` mostra reservas sem previsão** — o job pode ainda estar processando (espere e rode de novo). Se persistir, o tópico `previsoes` foi recriado sem apagar o checkpoint: siga a seção 12.5 ou use `docker compose down -v`.

**`teste_resiliencia` falha em `docker compose kill`** — rode o script a partir de um terminal onde o comando `docker` funcione e confira que o serviço `streaming` está de pé (`docker compose ps -a`).

**`teste_carga` mostra "sem resposta" em todas as rodadas** — o job `streaming` não está processando. Veja `docker compose logs --tail 30 streaming`.

**Latências muito altas já na primeira rodada do teste de carga** — normal se o job acabou de subir; rode de novo depois de alguns minutos.

---

## Estrutura do projeto

```
trabalho_final_sd/
├── docker-compose.yml   # Kafka, Spark, treino e streaming
├── requirements.txt     # dependências Python da sua máquina (producer/consumer)
├── common/
│   ├── preprocessing.py # limpeza e features, compartilhadas entre treino e Spark
│   └── inferencia.py    # previsão do modelo, executada nos executors
├── data/                # dataset e reservas_replay.csv (gerados na Etapa 2)
├── model/
│   └── treinar_modelo.py  # preparo dos dados + treino (Etapa 2)
├── producer/
│   └── producer.py      # publica as reservas no Kafka (Etapa 3)
├── spark/
│   ├── Dockerfile             # imagem do Spark (Python, Java, bibliotecas fixas)
│   ├── requirements-spark.txt # versões fixas usadas nos containers
│   └── streaming_job.py       # job de streaming (Etapa 4)
├── consumer/
│   ├── coletor.py       # lê o tópico previsoes numa thread de fundo
│   ├── analise.py       # cálculos do painel (funções puras)
│   └── painel.py        # painel Streamlit (Etapa 5)
├── scripts/
│   ├── verificar_consistencia.py  # confere reservas x previsões (perdas e duplicatas)
│   ├── teste_resiliencia.py       # derruba o Spark no meio do processamento
│   └── teste_carga.py             # vazão e latência por taxa de chegada
├── ROTEIRO_DEMONSTRACAO.md  # roteiro da apresentação
├── .streamlit/config.toml  # configuração do Streamlit
└── README.md
```

## Próximas etapas

- [x] Etapa 1 — Infraestrutura (Kafka + Spark)
- [x] Etapa 2 — Preparo dos dados e treino do Random Forest
- [x] Etapa 3 — Producer
- [x] Etapa 4 — Spark Structured Streaming
- [x] Etapa 5 — Consumidor, painel e alertas
- [x] Etapa 6 — Testes e demonstração de resiliência
- [ ] Etapa 7 — Documentação final
