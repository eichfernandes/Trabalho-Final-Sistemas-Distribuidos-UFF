# Trabalho Final — Sistemas Distribuídos

Previsão em tempo real da probabilidade de uma reserva de hotel se confirmar (base Hotel Booking Demand), usando **Kafka** e **Spark Structured Streaming**.

Fluxo: `Producer → Kafka (reservas) → Spark Streaming → Kafka (previsoes) → Consumidor`

> **Status:** em desenvolvimento. Este README é atualizado a cada etapa.
> Etapas concluídas até agora: **1 — Infraestrutura (Kafka + Spark)** **2 — Preparo dos dados e treino do modelo** e **3 — Producer**.

---

## 1. Pré-requisitos

| Item | Mínimo recomendado |
|---|---|
| Memória RAM | 8 GB (o cluster usa ~3 GB) |
| Espaço em disco | 5 GB livres |
| Internet | necessária na primeira execução (download das imagens Docker) |
| Docker + Docker Compose | instalação abaixo |
| Python 3.10 ou superior | a partir da Etapa 2 (seção 7) |

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
docker compose up -d
```

Na primeira vez, o Docker baixa as imagens do Kafka e do Spark (alguns minutos). O comando sobe:

| Serviço | Função |
|---|---|
| `kafka` | Broker Kafka (modo KRaft, sem Zookeeper) |
| `kafka-init` | Cria os tópicos `reservas` e `previsoes` (3 partições cada) e encerra |
| `spark-master` | Coordenador do cluster Spark |
| `spark-worker-1`, `spark-worker-2` | Executores (2 cores e 1 GB cada) |

### Portas utilizadas

| Porta | Uso |
|---|---|
| `29092` | Kafka — acesso a partir da sua máquina (producer/consumer) |
| `8080` | Interface web do Spark Master → <http://localhost:8080> |
| `8081` / `8082` | Interface web dos workers 1 e 2 |
| `7077` | Endereço do Spark Master |

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
| Zerar só o tópico `reservas` (apaga as mensagens) | `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic reservas` e depois `docker compose up kafka-init` (recria o tópico) |
| Listar tópicos | `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list` |
| Ver detalhes das partições | `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic reservas` |

---

## 7. Preparando o ambiente Python (Etapa 2 em diante)

Os scripts de dados, treino, producer e consumer rodam na **sua máquina** (não dentro do Docker), então é preciso ter o Python.

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

Esta etapa é executada **uma única vez** e não depende do Docker. Com o ambiente virtual ativo, na raiz do projeto:

```bash
python model/treinar_modelo.py
```

O script baixa o dataset *Hotel Booking Demand* do Kaggle (precisa de internet na primeira vez, ~15 MB), limpa os dados, treina o Random Forest e avalia. Leva de 1 a 3 minutos.

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
- O pré-processamento está em `common/preprocessing.py`, usado tanto no treino quanto (na Etapa 4) no Spark, para garantir que o modelo veja as mesmas features nos dois lugares.
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

O Kafka guarda as mensagens em disco. Se o producer for executado de novo, as reservas são publicadas outra vez. Para começar do zero, apague e recrie o tópico (comando na tabela da seção 6) ou use `docker compose down -v` (que apaga tudo, inclusive os outros tópicos) seguido de `docker compose up -d`.

---

## 10. Problemas comuns

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

---

## Estrutura do projeto

```
trabalho_final_sd/
├── docker-compose.yml   # Kafka + Spark (Etapa 1)
├── requirements.txt     # dependências Python (Etapa 2)
├── common/
│   └── preprocessing.py # limpeza e features, compartilhadas entre treino e Spark
├── data/                # dataset e reservas_replay.csv (gerados na Etapa 2)
├── model/
│   └── treinar_modelo.py  # preparo dos dados + treino (Etapa 2)
├── producer/
│   └── producer.py      # publica as reservas no Kafka (Etapa 3)
├── spark/               # job de streaming (Etapa 4)
├── consumer/            # painel e alertas (Etapa 5)
└── README.md
```

## Próximas etapas

- [x] Etapa 1 — Infraestrutura (Kafka + Spark)
- [x] Etapa 2 — Preparo dos dados e treino do Random Forest
- [x] Etapa 3 — Producer
- [ ] Etapa 4 — Spark Structured Streaming
- [ ] Etapa 5 — Consumidor, painel e alertas
- [ ] Etapa 6 — Testes e demonstração de resiliência
- [ ] Etapa 7 — Documentação final
