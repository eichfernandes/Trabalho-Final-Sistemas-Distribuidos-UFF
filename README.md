# Previsão de Cancelamento de Reservas em Tempo Real

> Trabalho Final de Sistemas Distribuídos — Kafka + Spark Structured Streaming + Machine Learning

## O que é este projeto

Hotéis só descobrem que uma reserva vai ser cancelada quando já é tarde para agir. Na base pública *Hotel Booking Demand*, cerca de 37% das reservas terminam em cancelamento ou no-show: cada uma é um quarto vazio e receita perdida.

Este projeto trata **cada reserva que chega como um evento** e devolve, na hora, a **probabilidade de o hóspede realmente chegar**. Com base nela, a equipe do hotel age antes do cancelamento:

| Probabilidade de chegar | Faixa | O que fazer |
|---|---|---|
| abaixo de 40% | Risco alto | Reserva provavelmente perdida: agir primeiro nela |
| de 40% a 60% | Risco médio | Vale ligar ou fazer uma oferta |
| acima de 60% | Risco baixo | Preservar a reserva |

**Como funciona, em uma linha:**

```
Producer → Kafka (reservas) → Spark Streaming + Random Forest → Kafka (previsoes) → Painel
```

1. Um **producer** reproduz as reservas do dataset em ordem cronológica e as publica no **Kafka**, no tópico `reservas`.
2. O **Spark Structured Streaming** lê esse tópico em micro-lotes, limpa os dados e aplica um **Random Forest** (treinado antes, com o histórico) em paralelo, em vários workers.
3. As previsões são publicadas em outro tópico do Kafka, `previsoes`.
4. Um **painel web** (Streamlit) lê esse tópico e mostra, ao vivo, os alertas por faixa de risco, a receita em risco, a qualidade do modelo e métricas do sistema distribuído.

**Tecnologias:** Apache Kafka (modo KRaft), Apache Spark 3.5 (PySpark, pandas UDF), scikit-learn (Random Forest), Streamlit, Docker Compose.

---

## Guia de leitura: o que você quer fazer?

| Eu quero... | Vá para |
|---|---|
| Instalar o que é preciso no meu computador | [2. Instalação](#2-instalação-uma-vez-por-computador) |
| **Rodar o projeto pela primeira vez** | [3. Primeira execução](#3-primeira-execução-passo-a-passo) |
| **Ligar de novo e usar nos dias seguintes** | [4. Uso no dia a dia](#4-uso-no-dia-a-dia-depois-da-primeira-vez) |
| **Zerar tudo e testar do zero** | [5. Zerar e testar do zero](#5-zerar-e-testar-do-zero) |
| Testar falhas, desempenho e reprocessamento | [6. Testes de resiliência e desempenho](#6-testes-de-resiliência-e-desempenho) |
| Entender como o projeto funciona por dentro | [7. Como o projeto funciona](#7-como-o-projeto-funciona) |
| Consultar portas e comandos úteis | [8. Referência rápida](#8-referência-rápida) |
| Resolver um erro | [9. Problemas comuns](#9-problemas-comuns) |
| Preparar a apresentação | [`ROTEIRO_DEMONSTRACAO.md`](ROTEIRO_DEMONSTRACAO.md) |

---

## 1. O que você precisa

| Item | Requisito |
|---|---|
| Sistema | Windows 10/11, macOS ou Linux |
| Memória RAM | 8 GB (o Docker precisa de pelo menos 6 GB disponíveis) |
| Espaço em disco | 8 GB livres (as imagens Docker somam ~3 GB) |
| Internet | Necessária na primeira execução (imagens Docker, dataset e conector do Kafka) |
| Docker + Docker Compose | Instalação na seção 2.1 |
| Python 3.10 a 3.13 | Instalação na seção 2.2 (usado pelo producer e pelo painel) |

> **Convenção deste guia:** os comandos estão no formato do **PowerShell** (Windows). No macOS/Linux, os comandos de `docker` são idênticos; as diferenças (ativar o ambiente virtual, apagar arquivos) estão indicadas.

---

## 2. Instalação (uma vez por computador)

### 2.1. Docker

#### Windows 10/11

1. Abra o **PowerShell como administrador** e instale o WSL 2:
   ```powershell
   wsl --install
   ```
   Reinicie o computador quando solicitado.
2. Baixe e instale o **Docker Desktop** em <https://www.docker.com/products/docker-desktop/>. Escolha a versão **AMD64** (Intel/AMD) ou **ARM64** (só para notebooks com processador ARM). Para descobrir: `echo $env:PROCESSOR_ARCHITECTURE` mostra `AMD64` ou `ARM64`.
3. Na instalação, mantenha marcada a opção **"Use WSL 2 instead of Hyper-V"**.
4. Abra o Docker Desktop e aguarde a mensagem *"Docker Desktop is running"*.
5. Dê memória ao Docker: crie (ou edite) o arquivo `%UserProfile%\.wslconfig` com:
   ```ini
   [wsl2]
   memory=6GB
   ```
   Depois rode `wsl --shutdown` no PowerShell e reabra o Docker Desktop.

> Se o `wsl --install` der o erro `Wsl/Service/E_UNEXPECTED` ou ficar pedindo "Enter new UNIX username" em loop, veja a [seção 9.1](#91-instalação-docker-e-wsl).

#### macOS

1. Baixe o **Docker Desktop** em <https://www.docker.com/products/docker-desktop/> (versão *Apple Silicon* ou *Intel*, conforme o seu Mac).
2. Arraste para *Applications*, abra e aceite os termos.
3. Em *Settings → Resources*, dê pelo menos **6 GB** de memória.

#### Linux (Ubuntu/Debian)

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

Para outras distribuições: <https://docs.docker.com/engine/install/>.

#### Conferindo a instalação

```powershell
docker --version
docker compose version
docker run --rm hello-world
```

O último comando deve imprimir *"Hello from Docker!"*. Se os três funcionarem, o Docker está pronto.

### 2.2. Python

- **Windows:** baixe o instalador em <https://www.python.org/downloads/> (versão 3.10 a 3.13). **Na primeira tela, marque a caixa "Add python.exe to PATH"** antes de clicar em *Install Now*.
- **macOS:** `brew install python` (com Homebrew) ou o instalador de python.org.
- **Linux:** `sudo apt-get install -y python3 python3-venv python3-pip`

Confira em um terminal **novo** (aberto depois da instalação):

```powershell
python --version
```

(No macOS/Linux, use `python3 --version`.) Deve mostrar a versão instalada.

### 2.3. Obtendo o projeto

Com Git:

```powershell
git clone <URL-DO-REPOSITORIO> trabalho_final_sd
```

Ou baixe o `.zip` do projeto e extraia. Daqui em diante, **"pasta do projeto"** é a pasta `trabalho_final_sd` (a que contém o arquivo `docker-compose.yml`).

### 2.4. Ambiente virtual do Python (uma vez)

O ambiente virtual (`.venv`) isola as bibliotecas do projeto, sem bagunçar o resto do computador. Crie-o **uma única vez**, na pasta do projeto:

```powershell
cd "C:\caminho\para\trabalho_final_sd"
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

(Se o caminho tiver espaços, as aspas são obrigatórias.)

**macOS / Linux:**
```bash
cd /caminho/para/trabalho_final_sd
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Quando o ambiente está ativo, aparece **`(.venv)`** no começo da linha do terminal. Se o PowerShell reclamar de *"execução de scripts desabilitada"*, rode uma vez `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`, responda `S` e repita a ativação.

> **Lembrete importante:** o ambiente virtual precisa ser **ativado em cada terminal novo** que for rodar Python (producer, painel, scripts). O comando é só `.venv\Scripts\Activate.ps1` (Windows) ou `source .venv/bin/activate` (macOS/Linux). Para sair dele, use `deactivate`.

---

## 3. Primeira execução (passo a passo)

Faça estes passos **na ordem**, na primeira vez que rodar o projeto. Depois da primeira vez, o dia a dia fica bem mais curto (seção 4).

### Antes de começar: os 3 terminais

O projeto usa **três terminais PowerShell abertos ao mesmo tempo**, todos na **pasta do projeto**:

| Terminal | Para quê | Python? |
|---|---|---|
| **Terminal 1** | Comandos do Docker (ligar, ver logs, desligar) | Não |
| **Terminal 2** | O painel (fica rodando) | Sim, com `(.venv)` ativo |
| **Terminal 3** | O producer e os scripts de teste | Sim, com `(.venv)` ativo |

**Como abrir um terminal já na pasta do projeto:**
- No Windows: abra a pasta no Explorador de Arquivos, clique na barra de endereço, digite `powershell` e aperte Enter; ou
- No VS Code: abra a pasta do projeto e use *Terminal → Novo Terminal* (e o botão **+** para abrir mais).

Por enquanto abra só o **Terminal 1**. Os outros entram quando forem necessários.

---

### Passo 1: Ligar o Docker e subir a infraestrutura (Terminal 1)

Abra o **Docker Desktop** e espere aparecer *"Docker Desktop is running"*. Depois:

```powershell
docker compose up -d --build
```

**Na primeira vez, isso leva de 5 a 10 minutos**: o Docker baixa a imagem do Kafka e *constrói* a imagem do Spark (Python, Java e bibliotecas de ML). Nas próximas vezes é questão de segundos.

Quando terminar, confira:

```powershell
docker compose ps -a
```

O esperado é:

| Container | Status esperado |
|---|---|
| `kafka` | `Up ... (healthy)` |
| `spark-master`, `spark-worker-1`, `spark-worker-2` | `Up` |
| `kafka-init` | `Exited (0)` — **é o correto**: ele só cria os tópicos `reservas` e `previsoes` e termina |

Abra <http://localhost:8080> no navegador: a página do Spark Master deve mostrar **2 workers** com status `ALIVE` (se mostrar 0, espere 20 segundos e atualize).

---

### Passo 2: Treinar o modelo (Terminal 1)

O modelo de IA (Random Forest) precisa ser treinado **uma vez**. O treino roda dentro do Docker, para usar exatamente as mesmas versões de bibliotecas do Spark:

```powershell
docker compose run --rm treino
```

Ele baixa o dataset *Hotel Booking Demand* do Kaggle (precisa de internet), limpa os dados, treina o modelo e avalia. **Leva de 1 a 4 minutos.** A saída passa por cinco blocos (`=== 1. Carga e limpeza ===` até `=== 5. Salvando artefatos ===`) e termina com **`Concluido.`**

Na parte `=== 4. Avaliacao no conjunto de teste ===`, o **ROC-AUC** deve ficar **acima de 0,90**. Aparecem também a acurácia, a matriz de confusão e quantas reservas caem em cada faixa de risco.

Confira que os arquivos foram criados:

```powershell
dir model
dir data
```

Devem existir `model\modelo_rf.joblib`, `model\metadados.json` e, em `data\`, `hotel_bookings.csv` e `reservas_replay.csv`.

> **Se o download do dataset falhar:** baixe o arquivo manualmente em <https://www.kaggle.com/datasets/jessemostipak/hotel-booking-demand>, copie o `hotel_bookings.csv` para a pasta `data\` e rode o comando de novo.

---

### Passo 3: Iniciar o job de streaming (Terminal 1)

O job do Spark é o "cérebro" do pipeline: lê as reservas do Kafka e devolve as previsões.

```powershell
docker compose up -d streaming
```

Ele leva de **30 a 60 segundos** para ficar pronto (na primeira vez, baixa também o conector do Kafka). Acompanhe com:

```powershell
docker compose logs --tail 5 streaming
```

Repita o comando até aparecer esta linha:

```
Streaming ativo: 'reservas' -> 'previsoes' (micro-lotes a cada 2s, ate 1000 reservas por lote).
```

Os avisos `WARN` (por exemplo, `Current batch is falling behind` no primeiro lote) são normais. O job agora fica em segundo plano esperando reservas chegarem.

---

### Passo 4: Abrir o painel (Terminal 2)

**Abra um novo terminal** (Terminal 2), entre na pasta do projeto e ative o ambiente virtual:

```powershell
cd "C:\caminho\para\trabalho_final_sd"
.venv\Scripts\Activate.ps1
```

Confirme que apareceu `(.venv)` no início da linha. Então:

```powershell
streamlit run consumer/painel.py
```

O terminal mostra algo como `Local URL: http://localhost:8501`. **O navegador não abre sozinho: abra <http://localhost:8501> manualmente.** Se o Windows perguntar sobre o firewall, permita o acesso.

O painel começa com a mensagem *"Conectado ao Kafka. Aguardando previsões..."*. É normal: ainda não enviamos nenhuma reserva. **Deixe este terminal aberto** (o painel roda enquanto ele estiver aberto).

---

### Passo 5: Enviar as reservas (Terminal 3)

**Abra mais um terminal** (Terminal 3), entre na pasta do projeto e ative o ambiente virtual (os mesmos dois comandos do passo anterior). Depois:

```powershell
python producer/producer.py --taxa 50
```

O producer envia as reservas, em ordem cronológica, a **50 por segundo**. O replay completo tem cerca de 17 mil reservas e leva uns **6 minutos**. Para um teste mais curto, use `--limite 500` (cerca de 10 segundos).

Durante o envio, aparecem linhas como `1500 enviadas | 50.1 reservas/s`. Ao final, o resumo deve mostrar `Enviadas` igual a `Entregues` e `Erros: 0`, com mensagens distribuídas pelas **3 partições**:

```
=== Resumo ===
Enviadas: 500 | Entregues e confirmadas pelo Kafka: 500 | Erros: 0
Mensagens por particao: {0: 165, 1: 170, 2: 165}
```

(Os números por partição variam.) Se quiser interromper antes, aperte `Ctrl+C`.

---

### Passo 6: Ver o projeto funcionando

Enquanto o producer envia, olhe o painel em <http://localhost:8501> (ele se atualiza sozinho a cada 2 segundos). Use o seletor de seções no topo:

| Seção | O que olhar |
|---|---|
| **Topo (sempre visível)** | Reservas processadas subindo, contagem por faixa de risco e a **receita estimada em risco alto** |
| **Visão geral** | Insights automáticos, risco por tipo de depósito/segmento/hotel e efeito da antecedência da reserva |
| **Alertas** | As reservas de risco alto e médio, da mais crítica para a menos. É o que a equipe do hotel usaria para ligar ou fazer uma oferta |
| **Qualidade do modelo** | Compara previsão com realidade. A taxa real de comparecimento deve **crescer de risco alto para risco baixo** |
| **Sistema distribuído** | Vazão, **latência de ponta a ponta** (alguns segundos) e as mensagens distribuídas pelas 3 partições |

Outros lugares para ver o sistema por dentro:
- <http://localhost:4040>: o job de streaming (abas *Structured Streaming* e *Executors*: as tarefas divididas entre os 2 workers).
- <http://localhost:8080>: o cluster Spark, com a aplicação `previsao-reservas` em execução.
- `docker compose logs -f streaming` (Terminal 1): uma linha `lote N | X reservas | ... reservas/s` a cada micro-lote. Para sair dos logs, `Ctrl+C` (o job continua rodando).

---

### Passo 7: Conferir que nada foi perdido (Terminal 3)

Quando o producer terminar, espere uns 10 segundos e rode:

```powershell
python scripts/verificar_consistencia.py
```

O esperado:

```
Reservas SEM previsao (perdidas): 0
Previsoes REPETIDAS (duplicatas): 0

RESULTADO: OK - toda reserva gerou exatamente uma previsao.
```

Se aparecer `FALHA` logo depois do producer, é só o Spark ainda processando: espere um pouco e rode de novo.

---

### Passo 8: Desligar

1. No **Terminal 2** (painel): `Ctrl+C`.
2. No **Terminal 1**:
   ```powershell
   docker compose down
   ```

Isso desliga os containers, mas **mantém os dados** (veja a seção 4). Pronto: você completou a primeira execução.

### Checklist: está tudo certo?

- [ ] `docker compose ps -a` mostra `kafka` healthy e os três containers do Spark `Up`
- [ ] O treino terminou com `Concluido.` e o ROC-AUC ficou acima de 0,90
- [ ] O log do `streaming` mostra `Streaming ativo`
- [ ] O painel mostra as reservas chegando e as 5 seções funcionam
- [ ] `verificar_consistencia.py` informa **0 reservas perdidas**

---

## 4. Uso no dia a dia (depois da primeira vez)

Depois da primeira execução, o modelo já está treinado e as imagens já foram construídas, então **ligar o projeto é rápido** (cerca de 1 minuto). Você **não** precisa repetir o treino nem o `--build`.

### Ligar (Terminal 1)

Abra o **Docker Desktop**, espere ele ficar pronto e rode:

```powershell
docker compose up -d
docker compose up -d streaming
docker compose logs --tail 5 streaming
```

Repita o último comando até aparecer `Streaming ativo: 'reservas' -> 'previsoes' ...` (leva uns 30 a 60 segundos).

### Abrir o painel (Terminal 2)

```powershell
cd "C:\caminho\para\trabalho_final_sd"
.venv\Scripts\Activate.ps1
streamlit run consumer/painel.py
```

Abra <http://localhost:8501>.

### Enviar reservas (Terminal 3)

```powershell
cd "C:\caminho\para\trabalho_final_sd"
.venv\Scripts\Activate.ps1
python producer/producer.py --taxa 50
```

### Desligar

1. `Ctrl+C` no painel (Terminal 2) e, se estiver rodando, no producer (Terminal 3).
2. No Terminal 1:
   ```powershell
   docker compose down
   ```

### O que acontece com os dados quando eu desligo?

Isso é importante para não se confundir ao ligar de novo:

| Comando | Containers | Reservas e previsões no Kafka | Estado do Spark (checkpoint) | Modelo e dados (pastas `model/` e `data/`) | Imagens Docker |
|---|---|---|---|---|---|
| `docker compose down` | removidos | **mantidas** | **mantido** | mantidos | mantidas |
| `docker compose down -v` | removidos | **apagadas** | **apagado** | mantidos | mantidas |
| `docker compose down -v --rmi all` | removidos | apagadas | apagado | mantidos | apagadas |

Na prática:

- **Se você usou `docker compose down` (sem `-v`)**, ao ligar de novo as reservas e previsões da vez anterior **continuam lá**. O painel lê tudo desde o começo e mostra o histórico imediatamente, mesmo antes de enviar reservas novas. Isso é o Kafka guardando as mensagens em disco.
- **Se você rodar o producer outra vez sem zerar**, as mesmas reservas são publicadas de novo (os ids se repetem). O sistema funciona normalmente, e o painel ignora as repetidas pelo `id_reserva`, mas os contadores de `reservas` no Kafka dobram. Para uma demonstração limpa, **zere antes** (próxima seção).
- Só quer rever o painel com os dados que já existem? Ligue o Docker e o `streaming` e abra o painel, sem rodar o producer.

---

## 5. Zerar e testar do zero

Há dois níveis de "zerar". Escolha o que precisa.

### 5.1. Reset rápido: apagar dados e recomeçar (o mais usado)

Apaga todas as reservas, previsões e o estado do Spark. **Mantém** o modelo treinado, o dataset e as imagens Docker (então é rápido). Use antes de cada teste ou demonstração.

**1.** Se o painel estiver aberto, pare-o: `Ctrl+C` no Terminal 2 (e o producer, se estiver rodando).

**2.** No Terminal 1:

```powershell
docker compose down -v
docker compose up -d
docker compose up -d streaming
docker compose logs --tail 5 streaming
```

Repita o último comando até aparecer `Streaming ativo`. (Como o `-v` apaga também o cache do conector do Kafka, o job leva ~1 minuto a mais para subir.)

**3.** Abra o painel de novo (Terminal 2: `streamlit run consumer/painel.py`). Ele começa vazio: *"Aguardando previsões"*.

**4.** Envie as reservas (Terminal 3: `python producer/producer.py --taxa 50`).

> Não deixe o painel aberto durante o reset. Se ele ficar aberto, mantém na tela os dados antigos; nesse caso, feche-o e abra de novo (ou use o botão **Recarregar do início** na barra lateral).

### 5.2. Reset completo: voltar ao estado de "computador novo"

Apaga **tudo** o que foi gerado: containers, volumes, imagens Docker, modelo e arquivos de dados. Use para testar se o passo a passo funciona do zero (por exemplo, antes de entregar o trabalho). Depois você precisa refazer a [primeira execução](#3-primeira-execução-passo-a-passo), incluindo os downloads.

**1.** Pare o painel e o producer (`Ctrl+C`).

**2.** No Terminal 1 (apaga containers, volumes e imagens, inclusive a do Kafka):

```powershell
docker compose down -v --rmi all
```

**3.** Apague o modelo e os dados gerados:

```powershell
Remove-Item model\modelo_rf.joblib, model\metadados.json -ErrorAction SilentlyContinue
Remove-Item data\reservas_replay.csv, data\hotel_bookings.csv -ErrorAction SilentlyContinue
Remove-Item data\resultados_carga.csv -ErrorAction SilentlyContinue
```

(No macOS/Linux: `rm -f model/modelo_rf.joblib model/metadados.json data/*.csv`.)

**4.** *(Opcional, para testar também a instalação do Python)* apague o ambiente virtual:

```powershell
deactivate
Remove-Item -Recurse -Force .venv
```

**5.** Refaça tudo a partir da [seção 2.4](#24-ambiente-virtual-do-python-uma-vez) (se apagou o `.venv`) ou da [seção 3](#3-primeira-execução-passo-a-passo).

### 5.3. Só recalcular as previsões (sem apagar as reservas)

Para mostrar o **reprocessamento** (o Kafka guardou as reservas, então o Spark pode recalcular tudo, por exemplo com um modelo novo), veja a [seção 6.5](#65-reprocessamento-o-kafka-guarda-as-mensagens).

---

## 6. Testes de resiliência e desempenho

Estes testes reúnem as evidências de sistemas distribuídos: tolerância a falhas, reprocessamento, vazão e latência. Rode-os no **Terminal 3** (com `(.venv)` ativo), com o Docker e o job `streaming` no ar. Para os números ficarem limpos, **zere antes** (seção 5.1) e rode um teste de cada vez.

### 6.1. Garantias de entrega: o que o pipeline promete

| Etapa do fluxo | Garantia |
|---|---|
| Producer → Kafka | Cada mensagem é confirmada pelo Kafka; envio idempotente (sem duplicar por reenvio de rede) |
| Kafka → Spark | O *checkpoint* guarda até onde o Spark leu: após uma queda, ele continua dali e **nenhuma reserva é perdida** |
| Spark → Kafka (`previsoes`) | **Pelo menos uma vez**: se o job cair no meio de um lote, esse lote pode ser escrito de novo, gerando previsões repetidas |
| Painel | Idempotente: se a mesma reserva aparece duas vezes, vale a última previsão, e o painel avisa quantas ignorou |

### 6.2. Verificar a consistência (a qualquer momento)

```powershell
python scripts/verificar_consistencia.py
```

Lê os tópicos `reservas` e `previsoes` inteiros e informa quantas reservas **ficaram sem previsão** (perda, que não deve acontecer) e quantas previsões estão **repetidas**. Se o job ainda estiver processando, espere alguns segundos e rode de novo.

### 6.3. Teste de falha: derrubar o Spark no meio

```powershell
python scripts/teste_resiliencia.py
```

O script (leva de 2 a 3 minutos) executa cinco etapas, narrando cada uma na tela:

1. Inicia o producer (3000 reservas, 50 por segundo).
2. Após 20 segundos, **mata o container do Spark** (`SIGKILL`: uma queda abrupta).
3. Mantém o Spark fora do ar por 15 segundos; as reservas continuam chegando e **ficam guardadas no Kafka**.
4. Religa o Spark, que retoma do checkpoint (leva uns 20 a 30 segundos para voltar).
5. Espera o processamento terminar e mostra o relatório de consistência.

O esperado é **`Reservas SEM previsao (perdidas): 0`**. Pode ou não aparecer algumas previsões repetidas (no máximo o lote em andamento na queda); o painel as ignora.

Exemplo de resultado obtido (varia conforme o momento da queda):

```
[2] DERRUBANDO o Spark (SIGKILL). Ate agora: 878 reservas enviadas, 829 previsoes prontas.
    reservas no Kafka: 1709 | previsoes: 829 (paradas)
    previsoes prontas: 3000/3000
Reservas SEM previsao (perdidas): 0
RESULTADO: OK - toda reserva gerou exatamente uma previsao.
```

Dica: deixe o painel aberto na seção **Sistema distribuído** durante o teste. A vazão cai a zero quando o Spark morre e dispara quando ele volta.

Opções: `--reservas`, `--taxa`, `--derrubar-apos`, `--pausa`.

### 6.4. Teste de carga: vazão e latência

```powershell
python scripts/teste_carga.py
```

Leva de 3 a 4 minutos. Testa taxas crescentes de chegada (20, 100, 300 e 600 reservas por segundo, 20 s cada) e mede a vazão de saída e a latência de ponta a ponta (p50, p95, p99), salvando em `data\resultados_carga.csv`.

Exemplo de resultado (a mesma máquina, configuração padrão):

| Taxa de chegada | Vazão de saída | Latência p50 / p95 | Situação |
|---|---|---|---|
| 20/s | 20/s | 2,0 s / 2,9 s | ok |
| 100/s | 109/s | 2,0 s / 3,0 s | ok |
| 300/s | 325/s | 1,8 s / 2,7 s | ok |
| 600/s | 484/s | 3,7 s / 6,0 s | no limite |

**Como ler:** até 300/s o sistema acompanha a chegada, com latência de cerca de 2 segundos (esse piso é o intervalo do micro-lote). Em 600/s a vazão de saída estabiliza perto de 500/s e a latência sobe: é o **ponto de saturação**. A capacidade teórica é `max-por-lote ÷ intervalo` = 1000 ÷ 2 s = **500 reservas/s**. Com taxas maiores e rodadas mais longas (`--taxas 600 800 1000 --duracao 40`), a fila no Kafka cresce e a latência dispara.

**Aumentando a capacidade** (ou reduzindo a latência mínima): o tamanho do lote e o intervalo são configuráveis. No PowerShell, por exemplo:

```powershell
$env:MAX_POR_LOTE=4000; $env:INTERVALO=1; docker compose up -d streaming
python scripts/teste_carga.py --taxas 300 600 1000
```

Para voltar ao padrão:

```powershell
Remove-Item Env:MAX_POR_LOTE, Env:INTERVALO
docker compose up -d streaming
```

### 6.5. Reprocessamento: o Kafka guarda as mensagens

O Kafka mantém as reservas em disco, então é possível apagar todos os resultados e recalculá-los. Com o `streaming` já tendo processado reservas, no **Terminal 1**, um comando por vez:

```powershell
docker compose stop streaming
docker compose rm -f streaming
docker volume rm trabalho_final_sd_checkpoints
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic previsoes
docker compose up kafka-init
docker compose up -d streaming
```

Isso apaga o checkpoint e o tópico de resultados (`previsoes`), mas **não** o de entrada (`reservas`). Ao iniciar sem checkpoint, o Spark lê o `reservas` desde o começo e gera todas as previsões de novo. Espere o `Streaming ativo` aparecer nos logs e confira no Terminal 3:

```powershell
python scripts/verificar_consistencia.py
```

Reservas e previsões devem coincidir. Se o painel estava aberto, use o botão **Recarregar do início**.

> O nome do volume (`trabalho_final_sd_checkpoints`) vem do nome da pasta do projeto. Se você renomear a pasta, confira o nome com `docker volume ls`.

### 6.6. O que cada teste demonstra

| Conceito de sistemas distribuídos | Onde aparece |
|---|---|
| Desacoplamento produtor/consumidor | Producer e Spark são independentes; o Spark pode cair sem o producer parar (6.3) |
| Persistência e reprocessamento | O Kafka guarda as reservas em disco (6.5) |
| Tolerância a falhas | Checkpoint do Spark (6.3) |
| Semântica de entrega | Pelo menos uma vez no destino + consumidor idempotente (6.1) |
| Paralelismo | Partições do Kafka → tarefas do Spark em 2 workers (painel e <http://localhost:4040>) |
| Escalabilidade e saturação | Teste de carga e ajuste do lote (6.4) |

---

## 7. Como o projeto funciona

### 7.1. As peças

| Peça | O que faz | Onde roda | Arquivo principal |
|---|---|---|---|
| **Treino** | Limpa o dataset, treina o Random Forest e gera o arquivo de reservas para simulação | Docker (sob demanda) | `model/treinar_modelo.py` |
| **Producer** | Lê o arquivo de simulação e publica cada reserva no Kafka, em ordem cronológica | Sua máquina | `producer/producer.py` |
| **Kafka** | Recebe e guarda as mensagens; divide a carga em 3 partições (tópicos `reservas` e `previsoes`) | Docker | `docker-compose.yml` |
| **Spark Streaming** | Lê `reservas` em micro-lotes, limpa os dados, aplica o modelo nos workers e escreve em `previsoes` | Docker (1 master, 2 workers e o job) | `spark/streaming_job.py` |
| **Painel** | Lê `previsoes` e mostra alertas, insights, qualidade do modelo e métricas do sistema | Sua máquina | `consumer/painel.py` |

### 7.2. O caminho de uma reserva

1. O **producer** envia uma reserva ao Kafka como uma mensagem JSON (exemplo ilustrativo, abreviado):
   ```json
   {"id_reserva": "R000051", "hotel": "City Hotel", "lead_time": 120, "deposit_type": "Non Refund",
    "adults": 2, "adr": 85.5, "...": "...", "data_reserva": "2014-11-12", "desfecho_real": 0, "ts_envio_ms": 1793653547000}
   ```
   O campo `desfecho_real` (a reserva compareceu ou não) viaja junto **apenas** para o painel comparar previsão com realidade. O modelo nunca o enxerga.
2. O **Kafka** guarda a mensagem em uma das 3 partições do tópico `reservas` (a chave é o `id_reserva`, o que distribui a carga por igual).
3. O **Spark** lê as mensagens novas a cada 2 segundos (micro-lote). Cada partição do Kafka vira uma tarefa, executada em paralelo nos workers. Em cada uma: descarta mensagens corrompidas e reservas inválidas, calcula as variáveis do modelo e aplica o Random Forest (pandas UDF).
4. O Spark publica o resultado no tópico `previsoes` (exemplo):
   ```json
   {"id_reserva": "R000051", "probabilidade": 0.2314, "faixa_risco": "alto", "desfecho_real": 0,
    "deposit_type": "Non Refund", "lead_time": 120, "noites": 3, "adr": 85.5, "...": "..."}
   ```
   `probabilidade` é a chance de o hóspede **chegar**; por isso uma probabilidade baixa significa risco **alto**.
5. O **painel** lê o tópico `previsoes` e atualiza a tela.

### 7.3. Decisões de projeto

- **Treino offline, inferência em streaming.** Um Random Forest não aprende de forma incremental: ele é treinado uma vez e só a previsão roda em tempo real.
- **Replay só com dados de teste.** O modelo é treinado com 80% das reservas; o simulador usa os 20% restantes, que o modelo nunca viu. Assim a acurácia mostrada no painel é honesta.
- **Sem vazamento de dados.** O resultado real (`desfecho_real`) e o status final da reserva ficam fora das variáveis do modelo.
- **Ordem cronológica.** A data da reserva é reconstruída como `data de chegada − antecedência (lead_time)`, o que dá uma ordem realista de chegada dos eventos.
- **Mesmo código no treino e no Spark.** A limpeza e as variáveis ficam em `common/preprocessing.py`, usado pelos dois.
- **Treino dentro do Docker.** O arquivo do modelo só é confiável com as mesmas versões de bibliotecas nos executors do Spark; treinar na mesma imagem garante isso.
- **Chave de partição = `id_reserva`.** Se fosse o tipo de hotel (só 2 valores), uma das 3 partições ficaria vazia.
- **Entrega "pelo menos uma vez" + consumidor idempotente.** Após uma falha, o Spark pode reescrever um lote; o painel ignora repetições pelo `id_reserva`.
- **Modelo carregado uma vez por executor**, e não a cada micro-lote, para a inferência ser rápida.

### 7.4. Glossário

| Termo | Significado |
|---|---|
| **Evento** | Algo que aconteceu e é registrado: aqui, a chegada de uma reserva |
| **Tópico** | "Caixa de correio" do Kafka onde as mensagens são publicadas (`reservas`, `previsoes`) |
| **Partição** | Pedaço de um tópico; permite dividir a carga e processar em paralelo |
| **Offset** | Posição de uma mensagem dentro de uma partição |
| **Producer / Consumer** | Quem publica mensagens / quem as lê |
| **Micro-lote** | Pequeno grupo de mensagens que o Spark processa de uma vez (a cada 2 s) |
| **Worker / Executor** | Processo do Spark que executa as tarefas; temos 2 workers |
| **pandas UDF** | Função Python aplicada em blocos de linhas, em paralelo, nos executors |
| **Checkpoint** | Anotação do Spark de até onde já leu no Kafka; permite retomar após uma falha |
| **Idempotente** | Que dá o mesmo resultado se a operação for repetida (ignorar uma mensagem repetida) |
| **Vazão** | Quantas reservas o sistema processa por segundo |
| **Latência** | Quanto tempo uma reserva leva do envio até a previsão chegar ao painel |

### 7.5. Estrutura de pastas

```
trabalho_final_sd/
├── docker-compose.yml       # Kafka, Spark, treino e streaming
├── requirements.txt         # dependências Python da sua máquina (producer e painel)
├── common/
│   ├── preprocessing.py     # limpeza e variáveis, compartilhadas entre treino e Spark
│   └── inferencia.py        # previsão do modelo, executada nos executors
├── data/                    # dataset e reservas_replay.csv (gerados pelo treino)
├── model/
│   └── treinar_modelo.py    # preparo dos dados + treino do modelo
├── producer/
│   └── producer.py          # publica as reservas no Kafka
├── spark/
│   ├── Dockerfile               # imagem do Spark (Python, Java, bibliotecas fixas)
│   ├── requirements-spark.txt   # versões fixas usadas nos containers
│   └── streaming_job.py         # job de streaming
├── consumer/
│   ├── coletor.py           # lê o tópico previsoes numa thread de fundo
│   ├── analise.py           # cálculos do painel
│   └── painel.py            # painel Streamlit
├── scripts/
│   ├── verificar_consistencia.py  # confere reservas x previsões (perdas e duplicatas)
│   ├── teste_resiliencia.py       # derruba o Spark no meio do processamento
│   └── teste_carga.py             # vazão e latência por taxa de chegada
├── .streamlit/config.toml   # configuração do Streamlit
├── ROTEIRO_DEMONSTRACAO.md  # roteiro da apresentação
└── README.md
```

Os arquivos em `data/` e `model/` (dataset, modelo e replay) **não vão para o GitHub**: são gerados pelo treino (passo 2 da seção 3).

---

## 8. Referência rápida

### Endereços e portas

| Endereço | O que é |
|---|---|
| <http://localhost:8501> | Painel Streamlit |
| <http://localhost:8080> | Spark Master (cluster, workers e aplicações) |
| <http://localhost:4040> | Job de streaming (só existe com o `streaming` rodando) |
| <http://localhost:8081> / <http://localhost:8082> | Workers 1 e 2 |
| `localhost:29092` | Kafka, acessado a partir da sua máquina (producer e painel) |
| `kafka:9092` | Kafka, acessado de dentro do Docker (Spark) |

Se alguma porta já estiver em uso no seu computador, feche o programa que a ocupa ou altere o lado esquerdo do mapeamento no `docker-compose.yml` (por exemplo, `"8090:8080"`).

### Serviços do Docker

| Serviço | Função | Sobe com `docker compose up -d`? |
|---|---|---|
| `kafka` | Broker Kafka | Sim |
| `kafka-init` | Cria os tópicos e termina (`Exited (0)` é normal) | Sim |
| `spark-master`, `spark-worker-1`, `spark-worker-2` | Cluster Spark | Sim |
| `streaming` | Job de streaming | **Não**: use `docker compose up -d streaming` |
| `treino` | Treino do modelo | **Não**: use `docker compose run --rm treino` |

### Comandos úteis (Terminal 1)

| Para... | Comando |
|---|---|
| Ver o estado dos containers | `docker compose ps -a` |
| Ver logs do job | `docker compose logs -f streaming` (saia com `Ctrl+C`) |
| Parar / religar só o job | `docker compose stop streaming` / `docker compose start streaming` |
| Listar os tópicos | `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list` |
| Ver as partições de um tópico | `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic reservas` |
| Contar mensagens por partição | `docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic reservas` |
| Ler 3 previsões | `docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic previsoes --from-beginning --max-messages 3` |

### Opções do producer

| Opção | Efeito | Padrão |
|---|---|---|
| `--taxa N` | N reservas por segundo (`0` = velocidade máxima) | 5 |
| `--limite N` | Envia só as N primeiras reservas | todas (~17 mil) |
| `--csv CAMINHO` | Usa outro arquivo de simulação | `data/reservas_replay.csv` |
| `--topico NOME` | Tópico de destino | `reservas` |
| `--bootstrap HOST:PORTA` | Endereço do Kafka | `localhost:29092` |

---

## 9. Problemas comuns

### 9.1. Instalação: Docker e WSL

**`wsl --install` dá `Wsl/Service/E_UNEXPECTED` ou fica pedindo "Enter new UNIX username" em loop (Windows)** — o Docker Desktop **não precisa** do Ubuntu. Saia do loop com `Ctrl+C`, reinicie o computador e, no PowerShell como administrador:
```powershell
wsl --update
wsl --shutdown
wsl --unregister Ubuntu
wsl --install --no-distribution
```
Reinicie de novo se pedir e instale o Docker Desktop. Se ainda falhar, confira se a **virtualização** está habilitada (Gerenciador de Tarefas → Desempenho → CPU → "Virtualização: Habilitada"; senão, ative na BIOS: *Intel VT-x*, *SVM Mode* ou *AMD-V*) e se os recursos **Plataforma de Máquina Virtual** e **Subsistema do Windows para Linux** estão marcados em "Ativar ou desativar recursos do Windows".

**`Cannot connect to the Docker daemon`** — o Docker não está rodando. Abra o Docker Desktop (Windows/macOS) ou `sudo systemctl start docker` (Linux).

**`permission denied` ao rodar `docker` no Linux** — `sudo usermod -aG docker $USER` e faça logout/login (ou use `sudo`).

**`port is already allocated`** — outra aplicação usa a porta. Veja a tabela de portas (seção 8).

**`kafka` fica `unhealthy` ou reinicia** — normalmente falta de memória. Dê pelo menos 6 GB ao Docker (seção 2.1) e rode `docker compose down -v` e `docker compose up -d`.

**Falha ao baixar imagens ou no `docker compose up --build` (apt ou pip)** — normalmente é rede. Tente de novo; se persistir, verifique proxy/VPN e se o Docker acessa a internet (`docker run --rm hello-world`).

### 9.2. Python

**`python` não é reconhecido (Windows)** — o Python não foi adicionado ao PATH. Reinstale marcando "Add python.exe to PATH", ou use `py` no lugar de `python`. Abra um terminal novo depois de instalar.

**O PowerShell bloqueia a ativação do `.venv` ("execução de scripts desabilitada")** — rode uma vez `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`, responda `S` e repita.

**`pip install` falha ao compilar algum pacote** — normalmente é uma versão do Python muito nova ou muito antiga. Use Python entre 3.10 e 3.13.

**`ModuleNotFoundError: No module named 'confluent_kafka'` (ou `pandas`, `streamlit`...)** — o ambiente virtual não está ativo neste terminal. Ative-o (`.venv\Scripts\Activate.ps1`) e, se faltar o pacote, rode `pip install -r requirements.txt`.

**`streamlit` não é reconhecido** — mesmo motivo (ambiente virtual inativo). Alternativa: `python -m streamlit run consumer/painel.py`.

### 9.3. Treino e dados

**O download do dataset falha** — baixe o arquivo em <https://www.kaggle.com/datasets/jessemostipak/hotel-booking-demand>, copie o `hotel_bookings.csv` para a pasta `data\` e rode `docker compose run --rm treino` de novo.

**`Arquivo nao encontrado: ...reservas_replay.csv` (producer)** — o treino ainda não foi executado (passo 2 da seção 3).

**`Modelo nao encontrado em model/` (log do `streaming`)** — falta rodar `docker compose run --rm treino`. Depois, `docker compose up -d streaming`.

### 9.4. Kafka e producer

**`Nao foi possivel conectar ao Kafka em localhost:29092`** — o Docker não está rodando ou os containers estão desligados. Rode `docker compose up -d` e confira `docker compose ps -a`.

**`O topico 'reservas' nao existe`** — o `kafka-init` não rodou. Execute `docker compose up kafka-init` e veja `docker compose logs kafka-init`.

**`Failed to acquire idempotence PID ... retrying` (aviso no producer)** — normal logo depois de subir o Kafka; ele resolve sozinho.

### 9.5. Spark e streaming

**Os workers não aparecem em <http://localhost:8080>** — aguarde ~20 segundos e atualize; se persistir, `docker compose logs spark-worker-1`.

**`Failed to find data source: kafka` ou erro ao baixar pacotes** — o conector do Kafka não foi baixado (sem internet no primeiro uso). Confira a conexão e rode `docker compose up -d streaming` de novo.

**`Initial job has not accepted any resources`** — os workers não estão disponíveis ou faltam recursos. Confira em <http://localhost:8080> se há 2 workers `ALIVE` e se o Docker tem pelo menos 6 GB de memória.

**Erro de offsets (`Partition reservas-0's offset was changed`...)** — o tópico foi recriado, mas o checkpoint antigo ficou. Zere tudo (seção 5.1): `docker compose down -v` e suba de novo.

**O `streaming` encerra logo após subir** — veja o motivo com `docker compose logs streaming`.

### 9.6. Painel

**"Sem conexão com o Kafka"** — o Docker não está rodando ou o Kafka está desligado. Rode `docker compose up -d` e confira `docker compose ps -a`. O painel tenta reconectar sozinho.

**Fica em "Aguardando previsões"** — conectou, mas o tópico `previsoes` está vazio. Confira se o `streaming` está rodando (`docker compose logs --tail 20 streaming`) e se o producer foi iniciado.

**A latência de ponta a ponta aparece como "—"** — nenhuma reserva chegou ao vivo ainda. Inicie o producer com o painel aberto. Ao abrir o painel depois, ele reconstrói o histórico, mas a latência só vale para reservas que chegam ao vivo.

**O painel mostra dados antigos depois de zerar** — pare o painel (`Ctrl+C`) e abra de novo, ou use o botão **Recarregar do início**.

**Porta 8501 em uso** — `streamlit run consumer/painel.py --server.port 8502`.

### 9.7. Testes

**`verificar_consistencia` mostra reservas sem previsão** — o job pode estar ainda processando: espere alguns segundos e rode de novo. Se persistir, o tópico `previsoes` foi recriado sem apagar o checkpoint: siga a seção 6.5 ou use `docker compose down -v` (seção 5.1).

**`teste_resiliencia` falha em `docker compose kill`** — rode o script na pasta do projeto, num terminal onde o `docker` funcione, e confira que o `streaming` está de pé (`docker compose ps -a`).

**`teste_carga` mostra "sem resposta" em todas as rodadas** — o job `streaming` não está processando. Veja `docker compose logs --tail 30 streaming`.

**Latências muito altas na primeira rodada do teste de carga** — normal se o job acabou de subir; rode de novo depois de alguns minutos.

---

## Para a apresentação

O roteiro completo (tempo de cada parte, o que mostrar, o que falar, perguntas prováveis do professor e limitações a admitir) está em [`ROTEIRO_DEMONSTRACAO.md`](ROTEIRO_DEMONSTRACAO.md).
