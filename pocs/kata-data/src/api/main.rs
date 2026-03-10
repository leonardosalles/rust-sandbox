use axum::{
    extract::State,
    http::StatusCode,
    response::{Html, Json},
    routing::get,
    Router,
};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use utoipa::OpenApi;
use utoipa::ToSchema;

use kata_data_rust::observability::{gather_metrics, init_metrics};


#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CityRanking {
    pub city: String,
    pub state: String,
    pub total_sales: f64,
    pub transaction_count: i64,
    pub rank: i64,
    pub computed_at: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CityRankingResponse {
    pub pipeline: String,
    pub count: usize,
    pub data: Vec<CityRanking>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct SalesmanRanking {
    pub salesman_id: String,
    pub salesman_name: String,
    pub total_sales: f64,
    pub transaction_count: i64,
    pub rank: i64,
    pub cities_served: i64,
    pub computed_at: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct SalesmanRankingResponse {
    pub pipeline: String,
    pub count: usize,
    pub data: Vec<SalesmanRanking>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct PipelineRun {
    pub run_id: String,
    pub pipeline: String,
    pub status: String,
    pub duration_ms: Option<i64>,
    pub records_in: Option<i64>,
    pub records_out: Option<i64>,
    pub ran_at: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct PipelineRunsResponse {
    pub runs: Vec<PipelineRun>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct HealthResponse {
    pub status: String,
    pub service: String,
    pub version: String,
    pub timestamp: String,
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "KataData 2026 API",
        version = "0.1.0",
        description = "REST API for KataData 2026 — Rust Data Pipeline.\n\nExposes aggregated results from two pipelines:\n- **Top Sales per City**: total sales ranked by city\n- **Top Salesman Country**: national salesman ranking\n\nData is ingested from 3 sources (SQLite DB, CSV filesystem, SOAP WS-*), processed with Apache DataFusion, and stored in SQLite.",
    ),
    paths(health, top_sales_per_city, top_salesman_country, pipeline_runs, prometheus_metrics),
    components(schemas(
        HealthResponse,
        CityRankingResponse, CityRanking,
        SalesmanRankingResponse, SalesmanRanking,
        PipelineRunsResponse, PipelineRun,
    )),
    tags(
        (name = "health",        description = "Service health check"),
        (name = "pipelines",     description = "Aggregated pipeline results"),
        (name = "observability", description = "Metrics and monitoring"),
    )
)]
struct ApiDoc;

#[derive(Clone)]
struct AppState {
    db_path: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(tracing_subscriber::fmt::layer().json())
        .init();

    init_metrics();

    let db_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "data/analytics.db".to_string());

    let state = Arc::new(AppState { db_path });

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let port = std::env::args()
        .nth(2)
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(8080);

    let app = Router::new()
        .route("/health",               get(health))
        .route("/top-sales-per-city",   get(top_sales_per_city))
        .route("/top-salesman-country", get(top_salesman_country))
        .route("/pipeline-runs",        get(pipeline_runs))
        .route("/metrics",              get(prometheus_metrics))
        .route("/openapi.json",         get(openapi_spec))
        .route("/docs",                 get(scalar_ui))
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port))
        .await
        .unwrap();

    info!("KataData API  >  http://0.0.0.0:{}", port);
    info!("  Docs     >  http://localhost:{}/docs", port);
    info!("  OpenAPI  >  http://localhost:{}/openapi.json", port);
    info!("  Health   >  http://localhost:{}/health", port);
    info!("  Metrics  >  http://localhost:{}/metrics", port);

    axum::serve(listener, app).await.unwrap();
}

#[utoipa::path(get, path = "/health", tag = "health",
    responses((status = 200, description = "Service is healthy", body = HealthResponse))
)]
async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status:    "ok".into(),
        service:   "kata-data-rust".into(),
        version:   "0.1.0".into(),
        timestamp: chrono::Utc::now().to_rfc3339(),
    })
}

#[utoipa::path(get, path = "/top-sales-per-city", tag = "pipelines",
    responses(
        (status = 200, description = "Cities ranked by total sales", body = CityRankingResponse),
        (status = 500, description = "Database error"),
    )
)]
async fn top_sales_per_city(
    State(state): State<Arc<AppState>>,
) -> Result<Json<CityRankingResponse>, StatusCode> {
    let conn = Connection::open(&state.db_path)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut stmt = conn
        .prepare("SELECT city, state, total_sales, transaction_count, rank, computed_at
                  FROM top_sales_per_city ORDER BY rank ASC")
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let data: Vec<CityRanking> = stmt
        .query_map([], |row| Ok(CityRanking {
            city:              row.get(0)?,
            state:             row.get(1)?,
            total_sales:       row.get(2)?,
            transaction_count: row.get(3)?,
            rank:              row.get(4)?,
            computed_at:       row.get(5)?,
        }))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .filter_map(|r| r.ok())
        .collect();

    let count = data.len();
    Ok(Json(CityRankingResponse { pipeline: "top_sales_per_city".into(), count, data }))
}

#[utoipa::path(get, path = "/top-salesman-country", tag = "pipelines",
    responses(
        (status = 200, description = "Salesmen ranked nationally", body = SalesmanRankingResponse),
        (status = 500, description = "Database error"),
    )
)]
async fn top_salesman_country(
    State(state): State<Arc<AppState>>,
) -> Result<Json<SalesmanRankingResponse>, StatusCode> {
    let conn = Connection::open(&state.db_path)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut stmt = conn
        .prepare("SELECT salesman_id, salesman_name, total_sales, transaction_count,
                         rank, cities_served, computed_at
                  FROM top_salesman_country ORDER BY rank ASC")
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let data: Vec<SalesmanRanking> = stmt
        .query_map([], |row| Ok(SalesmanRanking {
            salesman_id:       row.get(0)?,
            salesman_name:     row.get(1)?,
            total_sales:       row.get(2)?,
            transaction_count: row.get(3)?,
            rank:              row.get(4)?,
            cities_served:     row.get(5)?,
            computed_at:       row.get(6)?,
        }))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .filter_map(|r| r.ok())
        .collect();

    let count = data.len();
    Ok(Json(SalesmanRankingResponse { pipeline: "top_salesman_country".into(), count, data }))
}

#[utoipa::path(get, path = "/pipeline-runs", tag = "observability",
    responses(
        (status = 200, description = "Recent pipeline execution history", body = PipelineRunsResponse),
        (status = 500, description = "Database error"),
    )
)]
async fn pipeline_runs(
    State(state): State<Arc<AppState>>,
) -> Result<Json<PipelineRunsResponse>, StatusCode> {
    let conn = Connection::open(&state.db_path)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut stmt = conn
        .prepare("SELECT run_id, pipeline, status, duration_ms, records_in, records_out, ran_at
                  FROM pipeline_runs ORDER BY ran_at DESC LIMIT 50")
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let runs: Vec<PipelineRun> = stmt
        .query_map([], |row| Ok(PipelineRun {
            run_id:      row.get(0)?,
            pipeline:    row.get(1)?,
            status:      row.get(2)?,
            duration_ms: row.get(3)?,
            records_in:  row.get(4)?,
            records_out: row.get(5)?,
            ran_at:      row.get(6)?,
        }))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .filter_map(|r| r.ok())
        .collect();

    Ok(Json(PipelineRunsResponse { runs }))
}

#[utoipa::path(get, path = "/metrics", tag = "observability",
    responses((status = 200, description = "Prometheus metrics in text format"))
)]
async fn prometheus_metrics(
    State(state): State<Arc<AppState>>,
) -> (StatusCode, String) {
    let body = match build_metrics_from_db(&state.db_path) {
        Ok(s) => s,
        Err(_) => gather_metrics(),
    };
    (StatusCode::OK, body)
}

fn build_metrics_from_db(db_path: &str) -> anyhow::Result<String> {
    let conn = Connection::open(db_path)?;

    let mut stmt = conn.prepare(
        "SELECT pipeline, status, COUNT(*) FROM pipeline_runs GROUP BY pipeline, status",
    )?;
    let run_rows: Vec<(String, String, i64)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
        .filter_map(|r| r.ok())
        .collect();

    let mut stmt2 = conn.prepare(
        "SELECT pipeline, SUM(records_in) FROM pipeline_runs WHERE records_in IS NOT NULL GROUP BY pipeline",
    )?;
    let record_rows: Vec<(String, i64)> = stmt2
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .filter_map(|r| r.ok())
        .collect();

    let mut stmt3 = conn.prepare(
        "SELECT pipeline, AVG(duration_ms), MIN(duration_ms), MAX(duration_ms) \
         FROM pipeline_runs WHERE duration_ms IS NOT NULL GROUP BY pipeline",
    )?;
    let dur_rows: Vec<(String, f64, i64, i64)> = stmt3
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?
        .filter_map(|r| r.ok())
        .collect();

    let mut stmt4 = conn.prepare(
        "SELECT pipeline, ran_at FROM pipeline_runs \
         WHERE ran_at IN (SELECT MAX(ran_at) FROM pipeline_runs GROUP BY pipeline)",
    )?;
    let last_rows: Vec<(String, String)> = stmt4
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .filter_map(|r| r.ok())
        .collect();

    let mut out = String::new();

    out.push_str("# HELP kata_pipeline_runs_total Total pipeline executions\n");
    out.push_str("# TYPE kata_pipeline_runs_total counter\n");
    for (pipeline, status, cnt) in &run_rows {
        out.push_str(&format!(
            "kata_pipeline_runs_total{{pipeline=\"{}\",status=\"{}\"}} {}\n",
            pipeline, status, cnt
        ));
    }

    out.push_str("# HELP kata_records_processed_total Records ingested per pipeline\n");
    out.push_str("# TYPE kata_records_processed_total counter\n");
    for (pipeline, total) in &record_rows {
        out.push_str(&format!(
            "kata_records_processed_total{{pipeline=\"{}\"}} {}\n",
            pipeline, total
        ));
    }

    out.push_str("# HELP kata_pipeline_duration_ms_avg Average pipeline duration ms\n");
    out.push_str("# TYPE kata_pipeline_duration_ms_avg gauge\n");
    out.push_str("# HELP kata_pipeline_duration_ms_min Min pipeline duration ms\n");
    out.push_str("# TYPE kata_pipeline_duration_ms_min gauge\n");
    out.push_str("# HELP kata_pipeline_duration_ms_max Max pipeline duration ms\n");
    out.push_str("# TYPE kata_pipeline_duration_ms_max gauge\n");
    for (pipeline, avg, min, max) in &dur_rows {
        out.push_str(&format!("kata_pipeline_duration_ms_avg{{pipeline=\"{}\"}} {:.2}\n", pipeline, avg));
        out.push_str(&format!("kata_pipeline_duration_ms_min{{pipeline=\"{}\"}} {}\n", pipeline, min));
        out.push_str(&format!("kata_pipeline_duration_ms_max{{pipeline=\"{}\"}} {}\n", pipeline, max));
    }

    out.push_str("# HELP kata_pipeline_last_run_info Last run timestamp info\n");
    out.push_str("# TYPE kata_pipeline_last_run_info gauge\n");
    for (pipeline, ran_at) in &last_rows {
        out.push_str(&format!(
            "kata_pipeline_last_run_info{{pipeline=\"{}\",last_run=\"{}\"}} 1\n",
            pipeline, ran_at
        ));
    }

    Ok(out)
}

// SCALAR UI
async fn openapi_spec() -> Json<Value> {
    let json_str = ApiDoc::openapi().to_json().unwrap_or_default();
    Json(serde_json::from_str(&json_str).unwrap_or(Value::Null))
}

async fn scalar_ui() -> Html<String> {
    Html(r#"<!doctype html>
<html>
  <head>
    <title>KataData 2026 — API Docs</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
  </head>
  <body>
    <script
      id="api-reference"
      data-url="/openapi.json"
      data-configuration='{"theme":"purple","layout":"modern","defaultHttpClient":{"targetKey":"shell","clientKey":"curl"}}'
    ></script>
    <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
  </body>
</html>"#.into())
}
