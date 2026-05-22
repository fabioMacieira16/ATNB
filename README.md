# 🚗 Análise de Dados de Trânsito no Brasil

## 📌 Objetivo

Este projeto tem como objetivo analisar dados públicos de trânsito no Brasil utilizando técnicas de Big Data com Python.

A análise busca identificar padrões relacionados a:

* acidentes de trânsito
* distribuição geográfica
* causas de acidentes
* horários com maior incidência

---

## 📊 Fonte dos Dados

Os dados utilizados são provenientes de bases públicas:

* Polícia Rodoviária Federal (PRF)
* SENATRAN (Secretaria Nacional de Trânsito)
* Portal Brasileiro de Dados Abertos

Todos os dados são anonimizados e não contêm informações pessoais sensíveis.

---

## 🛠️ Tecnologias Utilizadas

* Python 3
* Pandas
* NumPy
* Plotly
* Streamlit

---

## 📁 Estrutura do Projeto

data/ → datasets utilizados
notebooks/ → análise exploratória
src/ → scripts de processamento
app/ → dashboard interativo

---

## 🔍 Etapas do Projeto

1. Coleta de dados públicos
2. Limpeza e tratamento dos dados
3. Análise exploratória
4. Cruzamento de dados
5. Visualização de dados
6. Criação de dashboard

---

## 📈 Visualizações

O projeto apresenta:

* gráficos de acidentes por estado
* análise por horário
* principais causas de acidentes
* distribuição geográfica

Todos os gráficos são interativos e executados no navegador.

---

## 🔄 Fluxo de Execução (Arquivos)

```
python -m src.pipeline.pipeline
         │
         ▼
src/pipeline/pipeline.py          ← Orquestrador principal
         │
         ├─► src/pipeline/ingestion.py   ← BRONZE: lê os CSVs brutos de data/
         │         data/acidentes2023.csv
         │         data/Vitimas_DadosAbertos_20260312.csv
         │         data/TipoVeiculo_DadosAbertos_20260312.csv
         │         data/Localidade_20260312.csv
         │         data/Volume_trafego_mensal.csv
         │
         ├─► src/pipeline/transform.py   ← SILVER: limpa e padroniza os dados
         │         Normaliza datas, horas, strings e remove nulos
         │
         ├─► src/pipeline/enrich.py      ← GOLD: cruza as fontes e gera analíticos
         │         acidentes + localidade (município/UF/habitantes/frota)
         │         agregação de vítimas, veículos, ranking de locais
         │         análise temporal e correlação de dados
         │
         ├─► src/pipeline/persist.py     ← Salva tudo como Parquet particionado
         │         data/processed/acidentes_gold/   (por ano e UF)
         │         data/processed/vitimas_silver/   (por ano)
         │         data/processed/analise_temporal/
         │         data/processed/...
         │
         └─► src/pipeline/ml.py          ← (opcional) Modelos de Machine Learning
                   Prevê gravidade de lesão por Decision Tree / MLP / SVC

streamlit run app/dashboard.py
         │
         ▼
app/dashboard.py                  ← Lê os Parquets e exibe gráficos no navegador
```

---

## ▶️ Como Executar

### 1. Instalar dependências

```bash
pip install -r requirements.txt
```

### 2. Rodar a Pipeline de Dados

Executa as camadas Bronze → Silver → Gold → Parquet:

```bash
python -m src.pipeline.pipeline
```

Opções disponíveis:

```bash
# Pula o arquivo TipoVeiculo (310 MB) para execução mais rápida
python -m src.pipeline.pipeline --skip-heavy

# Processa apenas um ano específico
python -m src.pipeline.pipeline --ano 2023
```

### 3. Rodar o Dashboard

```bash
streamlit run app/dashboard.py
```

---

## 🎯 Conclusões da Análise

### 📅 Evolução Temporal (2018–2025)
* O número de acidentes cresceu **+58%** entre 2018 (749 mil) e 2023 (1,18 milhão), pico histórico da série
* Apesar do crescimento, o número de mortes **caiu progressivamente** — de 26,5 mil em 2019 para 19,3 mil em 2025 — indicando avanços em atendimento e segurança veicular
* Em 2020 houve queda de ~10% nos acidentes (928 mil) em relação a 2019, reflexo das restrições de mobilidade impostas pela pandemia de COVID-19
* **Outubro** é consistentemente o mês mais perigoso (741 mil acidentes, 16,8 mil mortes); dezembro e janeiro registram os menores índices

### ⏰ Padrão Horário e Semanal
* O horário de maior incidência de acidentes é entre **17h e 19h** (saída do trabalho), com pico às 17h (615 mil acidentes)
* Entretanto, às **19h** a letalidade é mais alta: 13,2 mil mortes — provável combinação de cansaço e menor visibilidade
* **Domingo** concentra a maior letalidade da semana: apesar de ser o dia com menos acidentes (991 mil), registra o maior número de óbitos (41 mil), sugerindo influência de alta velocidade e consumo de álcool
* **Sexta-feira** lidera em volume total de acidentes (1,34 milhão)

### 📍 Distribuição Geográfica
* **Belo Horizonte (MG)** é o município com mais acidentes registrados (593 mil), porém com baixa mortalidade relativa
* **São Paulo (SP)** aparece em 3.º lugar em acidentes (299 mil), mas com mortalidade **6× maior** que BH (6,7 mil óbitos), apontando maior severidade dos acidentes
* **MG** lidera em volume de acidentes entre os estados (2,1 milhões), enquanto **SP** e **BA** apresentam as maiores taxas de mortalidade (10,5% e 8,3%, respectivamente)
* **SC e DF** possuem as menores taxas de mortalidade entre os estados com alto volume de acidentes (~1,2–1,4%)

### 🔗 Correlação Frota × Acidentes
* Coeficiente de Pearson **r = 0,686** entre frota circulante e total de acidentes — correlação moderada-forte
* Municípios com maior frota tendem a ter mais acidentes, mas a relação não é diretamente proporcional: cidades menores com frota reduzida podem apresentar taxas por 100 mil habitantes até mais elevadas

---

## 👨‍💻 Autor

Projeto acadêmico desenvolvido para disciplina de Big Data em Python.

Fábio Macieira
Josué Ferreira
Victor Martins


## Como rodar o projeto

### Criando o ambiente virtual

```bash
python -m venv .venv
```

### Ativando o ambiente

```bash
# Windows
.venv\Scripts\Activate.ps1

# Linux/macOS
source .venv/bin/activate
```

### Instalar as dependências

```bash
pip install -r requirements.txt
```

### Rodar a pipeline

```bash
python -m src.pipeline.pipeline
```

### Rodar o dashboard

```bash
streamlit run app/dashboard.py
```

### Desativar o ambiente quando terminar

```bash
deactivate
```

---

## 🌐 Como Compartilhar / Disponibilizar para Outras Pessoas

O projeto possui **três formas** de ser compartilhado, da mais simples à mais robusta.

---

### ✅ Opção 1 — GitHub + Google Drive (Recomendado para projetos acadêmicos)

Os dados brutos (~4,5 GB) e processados (~418 MB) são grandes demais para o GitHub.
A estratégia é: **código no GitHub + dados no Google Drive**.

**Passo 1 — Compactar os dados processados:**
```bash
# Windows (PowerShell)
Compress-Archive -Path data\processed -DestinationPath processed_data.zip

# Linux/macOS
zip -r processed_data.zip data/processed/
```

**Passo 2 — Fazer upload do ZIP para o Google Drive:**
1. Acesse [drive.google.com](https://drive.google.com)
2. Faça upload do arquivo `processed_data.zip`
3. Clique com o botão direito → **Compartilhar** → "Qualquer pessoa com o link"
4. Copie o **ID** do arquivo (parte do link entre `/d/` e `/view`)

**Passo 3 — Configurar o script de download:**

Abra o arquivo `setup_data.py` e substitua:
```python
FILE_ID = "COLE_AQUI_O_ID_DO_ARQUIVO_NO_GOOGLE_DRIVE"
```
pelo ID copiado no passo anterior.

**Quem receber o projeto faz apenas:**
```bash
git clone <url-do-repositorio>
cd ATNB
pip install -r requirements.txt
python setup_data.py        # baixa os dados processados
streamlit run app/dashboard.py
```

---

### ✅ Opção 2 — Streamlit Community Cloud (Dashboard online gratuito)

Permite publicar o dashboard em uma URL pública sem custo.

**Pré-requisitos:**
- Conta no [GitHub](https://github.com) com o código do projeto
- Dados processados disponíveis via Google Drive (execute o Passo 1 e 2 da Opção 1)

**Passos:**
1. Faça push do código para o GitHub (sem a pasta `data/` — já está no `.gitignore`)
2. Acesse [share.streamlit.io](https://share.streamlit.io) e faça login com sua conta GitHub
3. Clique em **"New app"** → selecione o repositório → defina `app/dashboard.py` como entry point
4. No menu **Advanced settings**, adicione uma variável de ambiente ou inclua o download automático dos dados no startup

> ⚠️ O Streamlit Cloud tem limite de ~1 GB de RAM. Para datasets grandes, considere disponibilizar apenas os Parquets agregados (ranking, temporal, correlação) que somam ~5 MB.

---

### ✅ Opção 3 — Docker (Rodar localmente sem instalar Python)

Ideal para quem não tem Python configurado ou quer uma instalação isolada.

**Requisito:** [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado.

```bash
# 1. Clone o repositório
git clone <url-do-repositorio>
cd ATNB

# 2. Baixe os dados processados (execute setup_data.py ou copie manualmente)
python setup_data.py

# 3. Suba o dashboard
docker compose up dashboard

# Acesse em: http://localhost:8501
```

Para rodar também a pipeline dentro do Docker:
```bash
docker compose --profile pipeline up pipeline
```

---

### 📋 Resumo das Opções

| | Opção 1 (GitHub + Drive) | Opção 2 (Streamlit Cloud) | Opção 3 (Docker) |
|---|---|---|---|
| **Custo** | Gratuito | Gratuito | Gratuito |
| **Online** | Não | ✅ Sim | Não |
| **Requer Python** | Sim | Não | Não |
| **Facilidade** | Média | Alta | Alta |
| **Ideal para** | Colaboradores técnicos | Apresentação / TCC | Qualquer máquina |