# CryptoMarketLakehouse 🚀

Um projeto robusto de **Engenharia de Dados (ETL/ELT)** demonstrando uma arquitetura moderna de Data Lakehouse usando **PySpark**, **Docker**, **PostgreSQL** e **Grafana**.
![alt text](image.png)
## 🎯 Objetivo
Ingerir dados de criptomoedas em tempo real, processá-los através de uma **Arquitetura Medalhão** (Bronze -> Silver -> Gold) para garantir qualidade e histórico, e servir dashboards interativos para análise financeira.

## 🏗️ Arquitetura

**Fluxo de Dados**:
`API (CoinGecko)` -> `Ingestão Python` -> `Landing Zone (JSON)` -> `PySpark (Bronze/Prata/Ouro)` -> `PostgreSQL` -> `Grafana`

**Camadas de Dados (Spark)**:
1.  **Bronze**: Dados brutos ingeridos preservando o timestamp original da fonte.
2.  **Silver**: Dados limpos, planificados (explode arrays), e tipados corretamente.
3.  **Gold**: Consultas de negócio otimizadas.
    *   `top_assets`: Snapshot dos Top 10 ativos atuais.
    *   `price_history`: Série temporal completa para análises de tendência.

## 🧠 Detalhamento Técnico

### 1. PySpark: O Cérebro (Pipeline Medalhão)
*   **Bronze (`job_bronze.py`)**: Ingestão bruta. Correção crítica: conversão do timestamp da API (ms) para Timestamp Spark real.
*   **Silver (`job_silver.py`)**: Limpeza e normalização. Transforma JSONs complexos/aninhados em tabelas planares (`explode`) e aplica tipagem forte.
*   **Gold (`job_gold.py`)**: Agregação de valor. Gera duas visões: o "agora" (Snapshot) e o "sempre" (Histórico), permitindo tanto alertas rápidos quanto análise gráfica profunda.

### 2. Por que Kubernetes?
Embora usemos Docker Compose para dev, o design é "Cloud Native":
*   **Escalabilidade**: No K8s, o Spark pode subir 50 pods workers automaticamente se a carga aumentar.
*   **Self-Healing**: Se um processo morrer, o K8s o reinicia instantaneamente.
*   **Isolamento**: Garante que o processamento pesado não derrube o banco de dados.

**Pilha Tecnológica (Stack)**:
*   **Ingestão**: Python 3.10+ (Requests + AsyncIO), Dockerizado.
*   **Processamento**: Cluster Apache Spark Standalone (Master/Worker).
*   **Serving**: PostgreSQL 15 (Tabelas: `crypto_top_assets`, `crypto_price_history`).
*   **Visualização**: Grafana 10 (Provisionado com Dashboards Interativos).
*   **Infraestrutura**: Docker Compose para orquestração local.

## ✨ Funcionalidades do Dashboard
O dashboard `Crypto Market Pulse (Advanced)` no Grafana oferece:
*   **Multi-Seleção de Ativos**: Compare BTC, ETH e SOL simultaneamente.
*   **Gráfico de Velas (Candlestick)**: Análise OHLC (Open, High, Low, Close) com agregações de 15 minutos.
*   **Volume de Negociação**: Monitoramento de liquidez em tempo real.
*   **Tabela de Liderança**: Ranking atualizado por Market Cap.

## 🚀 Como Iniciar

### 1. Pré-requisitos
*   Docker Desktop instalado e rodando.

### 2. Executar o Projeto
O comando abaixo constrói as imagens e sobe todos os serviços (Spark, Postgres, Grafana, Ingestão):
```bash
cd infra/docker
docker-compose up --build
```

### 3. Acessar os Serviços
*   **📊 Grafana**: [http://localhost:3000](http://localhost:3000) (Usuário: `admin` / Senha: `admin`)
*   **⚡ Spark Master**: [http://localhost:8090](http://localhost:8090)
*   **🐘 PostgreSQL**: Porta `5432` (Usuário: `admin` / Senha: `password` / DB: `cryptodb`)

---
**Desenvolvido por**: Antigravity Agent & User
**Última Atualização**: Jan 2026
