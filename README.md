<div align="center">

<img src="https://capsule-render.vercel.app/api?type=waving&color=0:1F9BD4,50:2E75B6,100:16265F&height=200&section=header&text=CryptoMarketLakehouse&fontSize=46&fontColor=ffffff&fontAlignY=38&desc=Pipeline%20End-to-End%20de%20Engenharia%20de%20Dados%20para%20Criptomoedas&descAlignY=58&descSize=17" />

</div>

<div align="center">

[![Python](https://img.shields.io/badge/Python-3.10%2B-1F9BD4?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-2E75B6?style=for-the-badge&logo=postgresql&logoColor=white)](https://postgresql.org)
[![Docker](https://img.shields.io/badge/Docker-Compose-16265F?style=for-the-badge&logo=docker&logoColor=white)](https://docker.com)
[![Grafana](https://img.shields.io/badge/Grafana-23%20Painéis-F46800?style=for-the-badge&logo=grafana&logoColor=white)](https://grafana.com)

</div>

---

## 📊 Sobre o Projeto

> **CryptoMarketLakehouse** é um pipeline **end-to-end de engenharia de dados** para criptomoedas, implementando a **Arquitetura Medallion** (Bronze → Silver → Gold) com PySpark 3.5, coleta em tempo quase real via CoinGecko API e visualização avançada com Grafana.

O projeto demonstra as melhores práticas de engenharia de dados modernas: ingestão automatizada, transformação com Spark, armazenamento em Parquet e PostgreSQL, e 23 painéis de monitoramento em Grafana com **9 KPIs** calculados via Window Functions.

---

## 🏗️ Arquitetura Medallion

<div align="center">

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        CryptoMarketLakehouse                             │
│                                                                          │
│  CoinGecko API ──(polling 60s)──► BRONZE LAYER                          │
│                                       │                                  │
│                               Raw Parquet Files                          │
│                               (dados brutos)                             │
│                                       │                                  │
│                                       ▼                                  │
│                                SILVER LAYER                              │
│                                       │                                  │
│                     Limpeza, Normalização, Tipagem                       │
│                     Parquet particionado por data                        │
│                                       │                                  │
│                                       ▼                                  │
│                                 GOLD LAYER                               │
│                                       │                                  │
│              ┌────────────────────────┴────────────────────────┐        │
│              │         9 Outputs KPI (PySpark + Window Fn)     │        │
│              └────────────────────────┬────────────────────────┘        │
│                                       │                                  │
│                                       ▼                                  │
│                              PostgreSQL 16                               │
│                                       │                                  │
│                                       ▼                                  │
│                          Grafana (23 painéis) 📈                         │
└──────────────────────────────────────────────────────────────────────────┘
```

</div>

---

## 📐 KPIs Calculados — Gold Layer

<div align="center">

| # | KPI | Técnica PySpark | Descrição |
|:---:|:---|:---|:---|
| 1 | **Volatility** | `stddev()` Window | Desvio padrão de preço por janela temporal |
| 2 | **Price Changes** | `lag()` Window | Variação percentual entre períodos |
| 3 | **Moving Averages** | `avg()` Window rolling | Médias móveis de 7, 14 e 30 períodos |
| 4 | **Market Dominance** | `sum()` + ratio | Participação de mercado por moeda |
| 5 | **Liquidity Score** | `volume / market_cap` | Índice de liquidez relativa |
| 6 | **Anomaly Detection** | Z-score Window | Detecção de picos e quedas anômalas |
| 7 | **Momentum Index** | RSI simplificado | Força direcional do preço |
| 8 | **Spread Analysis** | `max - min` por janela | Análise de amplitude intraday |
| 9 | **Correlation Matrix** | `corr()` cruzado | Correlação entre pares de criptomoedas |

</div>

---

## 🐳 Infraestrutura Docker Compose

<details>
<summary>📦 Ver os 6 serviços Docker</summary>

<br>

```yaml
# docker-compose.yml — 6 serviços
services:
  spark-master:       # PySpark 3.5 — nó master do cluster standalone
    image: bitnami/spark:3.5
    ports: ["8080:8080", "7077:7077"]

  spark-worker:       # Worker do cluster Spark
    image: bitnami/spark:3.5
    depends_on: [spark-master]

  postgres:           # Banco de dados relacional para a Gold Layer
    image: postgres:16
    ports: ["5432:5432"]

  grafana:            # Dashboards — 23 painéis analíticos
    image: grafana/grafana:latest
    ports: ["3000:3000"]

  coingecko-poller:   # Serviço Python de coleta (polling 60s)
    build: ./services/poller
    depends_on: [spark-master]

  pipeline-runner:    # Orquestrador do pipeline Bronze→Silver→Gold
    build: ./services/pipeline
    depends_on: [postgres, spark-master]
```

</details>

---

## 🛠️ Stack de Tecnologias

<div align="center">

[![My Skills](https://skillicons.dev/icons?i=python,docker,postgresql&theme=dark)](https://skillicons.dev)

</div>

<div align="center">

![Python](https://img.shields.io/badge/Python-3.10+-1F9BD4?style=flat-square&logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-Window%20Functions-2E75B6?style=flat-square&logo=apachespark&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?style=flat-square&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-1F9BD4?style=flat-square&logo=docker&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-Dashboards-F46800?style=flat-square&logo=grafana&logoColor=white)
![Parquet](https://img.shields.io/badge/Parquet-Storage-16265F?style=flat-square&logoColor=white)
![CoinGecko](https://img.shields.io/badge/CoinGecko-API-8DC647?style=flat-square&logoColor=white)

</div>

---

## 📦 Instalação e Execução

### Pré-requisitos

```bash
# Clone o repositório
git clone https://github.com/fassir/pyspark_01.git
cd pyspark_01

# Verifique se Docker e Docker Compose estão instalados
docker --version
docker compose version
```

### Subindo toda a infraestrutura

```bash
# Sobe os 6 serviços simultaneamente
docker compose up -d

# Acompanhe os logs do pipeline
docker compose logs -f pipeline-runner
```

### Acessando as interfaces

```bash
# Spark Master UI
open http://localhost:8080

# Grafana (usuário: admin / senha: admin)
open http://localhost:3000

# PostgreSQL
psql -h localhost -U crypto_user -d crypto_lakehouse
```

### Executando o pipeline manualmente

```python
# src/pipeline/run_pipeline.py
from bronze.ingestion import CoinGeckoPoller
from silver.transform import SilverTransformer
from gold.kpis import GoldKPIBuilder

poller     = CoinGeckoPoller(interval_seconds=60)
silver     = SilverTransformer(spark_session)
gold_layer = GoldKPIBuilder(spark_session)

# Pipeline completo
raw_df    = poller.collect()
silver_df = silver.transform(raw_df)
gold_df   = gold_layer.compute_all_kpis(silver_df)
gold_layer.write_to_postgres(gold_df)
```

---

## ✨ Funcionalidades

<div align="center">

| Funcionalidade | Detalhe | Status |
|:---|:---|:---:|
| 🔄 Coleta em tempo quase real | Polling CoinGecko API a cada 60s | ✅ |
| 🥉 Camada Bronze | Raw data em Parquet sem transformação | ✅ |
| 🥈 Camada Silver | Limpeza, tipagem e normalização | ✅ |
| 🥇 Camada Gold | 9 KPIs calculados com Window Functions | ✅ |
| 📊 Grafana Dashboards | 23 painéis analíticos interativos | ✅ |
| 🐳 Docker Compose | Infraestrutura completa em 1 comando | ✅ |
| ⚡ PySpark Standalone | Cluster distribuído local | ✅ |
| 🗄️ PostgreSQL | Persistência relacional da Gold Layer | ✅ |
| 📁 Parquet Storage | Armazenamento colunar eficiente | ✅ |

</div>

---

## 📂 Estrutura de Arquivos

```
pyspark_01/
├── 📁 services/
│   ├── 📁 poller/
│   │   ├── coingecko_client.py     # Cliente CoinGecko API
│   │   └── polling_service.py      # Polling 60s → Bronze
│   └── 📁 pipeline/
│       └── orchestrator.py         # Orquestrador Bronze→Silver→Gold
├── 📁 src/
│   ├── 📁 bronze/
│   │   └── ingestion.py            # Ingestão e escrita Parquet
│   ├── 📁 silver/
│   │   └── transform.py            # Transformações PySpark
│   └── 📁 gold/
│       ├── kpis.py                 # Cálculo de KPIs (Window Functions)
│       └── postgres_writer.py      # Escrita no PostgreSQL
├── 📁 grafana/
│   └── dashboards/                 # 23 painéis JSON exportados
├── 📁 data/
│   ├── bronze/                     # Parquet raw
│   ├── silver/                     # Parquet transformado
│   └── gold/                       # Parquet KPIs
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## 👤 Autor

<div align="center">

<img src="https://github.com/fassir.png" width="100" style="border-radius:50%"/>

**Fabio Piassi**

[![GitHub](https://img.shields.io/badge/GitHub-fassir-1F9BD4?style=for-the-badge&logo=github&logoColor=white)](https://github.com/fassir)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Fabio%20Piassi-2E75B6?style=for-the-badge&logo=linkedin&logoColor=white)](https://linkedin.com/in/fassir)

*Física • Engenharia de Dados • PySpark • Cloud*

</div>

---

<div align="center">

<img src="https://capsule-render.vercel.app/api?type=waving&color=0:16265F,50:2E75B6,100:1F9BD4&height=120&section=footer" />

</div>
