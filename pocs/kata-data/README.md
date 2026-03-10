# KataData 2026 — Rust Data Pipeline

POC do Kata de Dados implementado em **Rust puro** com:

- **DataFusion** como engine de processamento (substitui Spark/Flink)
- **tokio** para async I/O
- **axum** para a REST API
- **SQLite** como banco de dados (source + analytics sink)
- **OpenTelemetry + Prometheus** para observabilidade
- **Data Lineage** customizado (modelo inspirado no OpenLineage)

---

## Arquitetura

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA SOURCES (Ingestion)                 │
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │  SQLite DB   │  │ CSV / Files  │  │  SOAP WS-* Mock  │  │
│  │ (Relational) │  │ (Filesystem) │  │  (Traditional)   │  │
│  └──────┬───────┘  └──────┬───────┘  └────────┬─────────┘  │
└─────────┼─────────────────┼───────────────────┼────────────┘
          │                 │                   │
          └─────────────────┴───────────────────┘
                            │
                    ┌───────▼────────┐
                    │   DataFusion   │  ← SQL engine in-process
                    │  (Processing)  │    sem Spark, sem JVM
                    └───────┬────────┘
                            │
              ┌─────────────┴─────────────┐
              │                           │
    ┌─────────▼──────────┐   ┌────────────▼───────────┐
    │  Pipeline 1:       │   │  Pipeline 2:            │
    │  Top Sales         │   │  Top Salesman           │
    │  per City          │   │  (Country-wide)         │
    └─────────┬──────────┘   └────────────┬────────────┘
              │                           │
              └─────────────┬─────────────┘
                            │
                    ┌───────▼────────┐
                    │  SQLite        │  ← analytics.db
                    │  (Sink/Output) │    top_sales_per_city
                    └───────┬────────┘    top_salesman_country
                            │
                    ┌───────▼────────┐
                    │   axum REST    │  ← :8080
                    │     API        │
                    └───────┬────────┘
                            │
              ┌─────────────┴────────────┐
              │                          │
    ┌─────────▼──────────┐   ┌───────────▼───────────┐
    │  Prometheus        │   │  Data Lineage         │
    │  /metrics          │   │  (OpenLineage-style)  │
    └────────────────────┘   └───────────────────────┘
```

---

## Requisitos

- **Rust 1.75+** (`rustup update stable`)
- **curl** (para health checks no script)

Opcional (para Docker):

- Docker + Docker Compose

---

## Rodando Local (sem Docker)

```bash
# 1. Clone e entre no projeto
git clone <repo>
cd kata-data-rust

# 2. Roda tudo (SOAP mock + pipeline + API)
./scripts/run-local.sh
```

Ou passo a passo:

```bash
# Terminal 1 — Mock SOAP WS-* server
RUST_LOG=info cargo run --bin mock-soap
# Rodando em: http://localhost:8081
# WSDL em:    http://localhost:8081/wsdl

# Terminal 2 — Rodar os pipelines
RUST_LOG=info cargo run --bin pipeline

# Terminal 3 — Subir a API
RUST_LOG=info cargo run --bin api
# Rodando em: http://localhost:8080
```

---

## Rodando com Docker Compose

```bash
docker compose up --build
```

Serviços:
| Serviço | Porta | Descrição |
|-------------|-------|----------------------------------|
| api | 8080 | REST API com resultados |
| mock-soap | 8081 | Mock WS-\* / SOAP legacy service |
| prometheus | 9090 | Scraping de métricas |
| grafana | 3000 | Dashboards (admin/admin) |

---

## Endpoints da API

```bash
# Health check
curl http://localhost:8080/health

# Pipeline 1: Top Vendas por Cidade
curl http://localhost:8080/top-sales-per-city | jq

# Pipeline 2: Top Vendedor Nacional
curl http://localhost:8080/top-salesman-country | jq

# Histórico de execuções (audit log)
curl http://localhost:8080/pipeline-runs | jq

# Prometheus metrics
curl http://localhost:8080/metrics
```

### Exemplo de resposta — Top Sales per City

```json
{
  "pipeline": "top_sales_per_city",
  "count": 15,
  "data": [
    {
      "city": "São Paulo",
      "state": "SP",
      "total_sales": 262000.00,
      "transaction_count": 7,
      "rank": 1,
      "computed_at": "2026-03-10T12:00:00Z"
    },
    ...
  ]
}
```

---

## Fontes de Dados (Ingestion)

| Fonte                       | Tipo          | Implementação                  |
| --------------------------- | ------------- | ------------------------------ |
| `source.db`                 | Relational DB | SQLite via `rusqlite`          |
| `regional_sales.csv`        | Filesystem    | CSV via crate `csv`            |
| `localhost:8081/soap/sales` | WS-\* SOAP    | Rust mock server + `quick-xml` |

---

## Processamento — DataFusion

O DataFusion é um **SQL query engine** em Rust (parte do projeto Apache Arrow). Ele funciona in-process, sem JVM, sem Spark cluster — ideal para POC com alto desempenho.

Pipeline SQL executado:

```sql
-- Pipeline 1: Top Sales per City
SELECT
    city, state,
    ROUND(SUM(amount), 2)    AS total_sales,
    COUNT(*)                  AS transaction_count,
    RANK() OVER (ORDER BY SUM(amount) DESC) AS rank
FROM unified_sales
GROUP BY city, state
ORDER BY total_sales DESC;

-- Pipeline 2: Top Salesman Country
SELECT
    salesman_id, salesman_name,
    ROUND(SUM(amount), 2)         AS total_sales,
    COUNT(*)                       AS transaction_count,
    RANK() OVER (ORDER BY SUM(amount) DESC) AS rank,
    COUNT(DISTINCT city)           AS cities_served
FROM unified_sales
GROUP BY salesman_id, salesman_name
ORDER BY total_sales DESC;
```

---

## Data Lineage

Implementação inspirada no modelo [OpenLineage](https://openlineage.io/):

```
Run → Job → Inputs (datasets) → Outputs (datasets)
```

Cada execução de pipeline registra:

- **Run ID** (UUID)
- **Job** (nome + namespace + descrição)
- **Inputs**: fonte, schema, row count, namespace
- **Outputs**: tabela de destino, row count
- **Duration** em ms
- **Custom facets** (metadados adicionais)

O grafo de linhagem pode ser consultado programaticamente:

```
sqlite://data/source.db:sales   ──reads──▶  job:top_sales_per_city
file://data/:regional_sales.csv ──reads──▶  job:top_sales_per_city
http://localhost:8081/soap:soap ──reads──▶  job:top_sales_per_city
job:top_sales_per_city          ──writes──▶ sqlite://analytics.db:top_sales_per_city
```

---

## Observabilidade

### Prometheus Metrics

| Metric                           | Tipo      | Descrição                              |
| -------------------------------- | --------- | -------------------------------------- |
| `kata_pipeline_runs_total`       | Counter   | Total de execuções por pipeline/status |
| `kata_pipeline_duration_seconds` | Histogram | Duração das execuções                  |
| `kata_records_processed_total`   | Counter   | Records por fonte/pipeline             |
| `kata_ingestion_errors_total`    | Counter   | Erros de ingestão por fonte            |
| `kata_active_pipelines`          | Gauge     | Pipelines ativos no momento            |

### OpenTelemetry Traces

Spans instrumentados com `#[instrument]`:

- `ingest_sqlite`
- `ingest_csv`
- `ingest_soap`
- `load_unified_table`
- `pipeline_top_sales_per_city`
- `pipeline_top_salesman_country`
- `sink_top_sales_per_city`
- `sink_top_salesman_country`

Logs em formato JSON estruturado:

```json
{
  "timestamp": "2026-03-10T12:00:00Z",
  "level": "INFO",
  "target": "kata_data_rust::processing",
  "fields": {
    "pipeline": "top_sales_per_city",
    "duration_ms": 142,
    "status": "success"
  },
  "span": { "name": "pipeline_top_sales_per_city" }
}
```

---

## Estrutura do Projeto

```
kata-data-rust/
├── src/
│   ├── main.rs               # Pipeline orchestrator (bin: pipeline)
│   ├── lib.rs                # Library root
│   ├── api/
│   │   └── main.rs           # REST API (bin: api)
│   ├── ingestion/
│   │   ├── mod.rs
│   │   ├── sqlite_source.rs  # Ingestion: Relational DB
│   │   ├── csv_source.rs     # Ingestion: Filesystem
│   │   └── soap_source.rs    # Ingestion: WS-* SOAP
│   ├── processing/
│   │   ├── mod.rs            # DataFusion pipelines
│   │   └── sink.rs           # SQLite analytics sink
│   ├── lineage/
│   │   └── mod.rs            # Data Lineage tracker
│   └── observability/
│       └── mod.rs            # OTel + Prometheus
├── mock-soap/
│   ├── Cargo.toml
│   └── src/main.rs           # Mock SOAP WS-* server
├── docker/
│   ├── Dockerfile.soap
│   ├── Dockerfile.pipeline
│   └── Dockerfile.api
├── config/
│   ├── prometheus.yml
│   └── grafana/provisioning/
├── scripts/
│   └── run-local.sh
├── data/                     # Generated at runtime
│   ├── source.db             # Source: Relational DB
│   ├── regional_sales.csv    # Source: Filesystem
│   └── analytics.db         # Sink: Aggregated results
├── Cargo.toml
└── docker-compose.yml
```

---

## Diferencial vs Java/Scala

| Aspecto | Java (colega) | Rust (esta POC)                |
| ------- | ------------- | ------------------------------ |
| Runtime | JVM (Spark)   | Nativo, zero overhead          |
| Startup | ~5-10s        | <100ms                         |
| Memory  | 512MB+        | ~20MB                          |
| Engine  | Spark SQL     | Apache DataFusion              |
| SOAP    | Apache CXF    | Rust nativo (axum + quick-xml) |
| Build   | sbt           | cargo                          |
| Deploy  | JAR + JVM     | Single binary                  |

---

## Decisões de Design

**Por que DataFusion em vez de Spark?**
DataFusion é o engine columnar do Apache Arrow, escrito em Rust. Suporta SQL completo com window functions, GROUP BY, JOINs — tudo que o Kata exige — rodando in-process sem cluster.

**Por que SQLite em vez de Redshift?**
O requisito explicitamente proíbe Redshift. SQLite é zero-infra, bundled no binário via `rusqlite`, e perfeito para POC. Em produção, trocar por PostgreSQL seria apenas mudar a connection string.

**Por que mock SOAP em Rust?**
Demonstra que o ecossistema Rust consegue consumir serviços legados WS-\* sem depender de frameworks Java. O mock serve WSDL real e respostas SOAP/XML válidas.
