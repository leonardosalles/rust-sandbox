mod ingestion;
mod lineage;
mod observability;
mod processing;

use anyhow::Result;
use std::collections::HashMap;
use tracing::{error, info, instrument};

use ingestion::{
    csv_source::ingest_from_csv,
    soap_source::ingest_from_soap,
    sqlite_source::{ingest_from_sqlite, seed_source_db},
};
use lineage::{
    csv_source_dataset, soap_source_dataset, sqlite_sink_dataset, sqlite_source_dataset,
    LineageTracker,
};
use observability::{
    init_metrics, init_tracing, record_ingestion_error, record_pipeline_run,
    record_records_processed, PipelineTimer,
};
use processing::{sink, DataPipeline};

const SOURCE_DB: &str = "data/source.db";
const ANALYTICS_DB: &str = "data/analytics.db";
const SOAP_URL: &str = "http://localhost:8081/soap/sales";

fn parse_csv_path_arg() -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    args.windows(2)
        .find(|w| w[0] == "--csv-path")
        .map(|w| w[1].clone())
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    init_metrics();

    info!("╔══════════════════════════════════════════╗");
    info!("║     KataData 2026 -      Data Pipeline   ║");
    info!("╚══════════════════════════════════════════╝");

    let csv_path_arg = parse_csv_path_arg();
    let csv_path: Option<&str> = csv_path_arg.as_deref();

    std::fs::create_dir_all("data")?;
    sink::init_analytics_db(ANALYTICS_DB)?;

    info!("--- Seeding SQLite source data ---");
    seed_source_db(SOURCE_DB)?;

    let lineage = LineageTracker::new();

    // Run pipelines
    run_top_sales_per_city_pipeline(&lineage, csv_path).await?;
    run_top_salesman_country_pipeline(&lineage, csv_path).await?;

    // Print lineage graph
    let graph = lineage.get_lineage_graph();
    info!("=== Data Lineage Graph ===");
    info!("Nodes: {}", graph.nodes.len());
    info!("Edges: {}", graph.edges.len());
    for edge in &graph.edges {
        info!("  {} --[{}]--> {}", edge.from, edge.label, edge.to);
    }

    info!("All pipelines complete. Start API with: cargo run --bin api");
    Ok(())
}

/// Pipeline 1
#[instrument(name = "pipeline_top_sales_per_city", skip(lineage))]
async fn run_top_sales_per_city_pipeline(lineage: &LineageTracker, csv_path: Option<&str>) -> Result<()> {
    let timer = PipelineTimer::start("top_sales_per_city");
    let mut all_records = Vec::new();
    let mut input_datasets = Vec::new();
    let mut total_ingested = 0u64;

    match ingest_from_sqlite(SOURCE_DB) {
        Ok((records, count)) => {
            record_records_processed("sqlite", "top_sales_per_city", count);
            total_ingested += count;
            input_datasets.push(sqlite_source_dataset("sales", count));
            all_records.extend(records);
        }
        Err(e) => {
            error!("SQLite ingestion failed: {}", e);
            record_ingestion_error("sqlite");
        }
    }

    match ingest_from_csv(csv_path) {
        Ok((records, count)) if count > 0 => {
            record_records_processed("csv", "top_sales_per_city", count);
            total_ingested += count;
            let label = csv_path.unwrap_or("data/regional_sales.csv");
            input_datasets.push(csv_source_dataset(label, count));
            all_records.extend(records);
        }
        Ok(_) => info!("CSV source returned 0 records — continuing without it"),
        Err(e) => {
            error!("CSV ingestion failed: {}", e);
            record_ingestion_error("csv");
        }
    }

    match ingest_from_soap(SOAP_URL).await {
        Ok((records, count)) => {
            record_records_processed("soap_ws", "top_sales_per_city", count);
            total_ingested += count;
            input_datasets.push(soap_source_dataset(count));
            all_records.extend(records);
        }
        Err(e) => {
            error!("SOAP ingestion failed (is mock-soap running?): {}", e);
            record_ingestion_error("soap_ws");
            info!("Continuing without SOAP data...");
        }
    }

    info!("Total records ingested from all sources: {}", total_ingested);

    let run_id = lineage.start_run(
        "top_sales_per_city",
        "kata-data",
        "Aggregates total sales and transaction counts per city, ranked descending",
        input_datasets,
    );

    let pipeline = DataPipeline::new().await?;
    pipeline.load_records(&all_records).await?;
    let results = pipeline.run_top_sales_per_city().await?;
    let result_count = results.len() as u64;

    sink::sink_top_sales_per_city(ANALYTICS_DB, &results)?;
    sink::record_pipeline_run_db(
        ANALYTICS_DB,
        &run_id,
        "top_sales_per_city",
        "success",
        None,
        Some(total_ingested),
        Some(result_count),
    )?;

    let duration_ms = timer.finish(true);
    record_pipeline_run("top_sales_per_city", "success");

    let mut facets = HashMap::new();
    facets.insert("total_input_records".into(), serde_json::json!(total_ingested));
    facets.insert("sources".into(), serde_json::json!(["sqlite", "csv", "soap_ws"]));
    lineage.complete_run(
        &run_id,
        vec![sqlite_sink_dataset("top_sales_per_city", result_count)],
        duration_ms,
        facets,
    );

    info!("✓ Pipeline top_sales_per_city: {} cities ranked", result_count);
    Ok(())
}

/// Pipeline 2
#[instrument(name = "pipeline_top_salesman_country", skip(lineage))]
async fn run_top_salesman_country_pipeline(lineage: &LineageTracker, csv_path: Option<&str>) -> Result<()> {
    let timer = PipelineTimer::start("top_salesman_country");
    let mut all_records = Vec::new();
    let mut input_datasets = Vec::new();
    let mut total_ingested = 0u64;

    if let Ok((records, count)) = ingest_from_sqlite(SOURCE_DB) {
        record_records_processed("sqlite", "top_salesman_country", count);
        total_ingested += count;
        input_datasets.push(sqlite_source_dataset("sales", count));
        all_records.extend(records);
    }

    if let Ok((records, count)) = ingest_from_csv(csv_path) {
        if count > 0 {
            record_records_processed("csv", "top_salesman_country", count);
            total_ingested += count;
            let label = csv_path.unwrap_or("data/regional_sales.csv");
            input_datasets.push(csv_source_dataset(label, count));
            all_records.extend(records);
        }
    }

    match ingest_from_soap(SOAP_URL).await {
        Ok((records, count)) => {
            record_records_processed("soap_ws", "top_salesman_country", count);
            total_ingested += count;
            input_datasets.push(soap_source_dataset(count));
            all_records.extend(records);
        }
        Err(e) => {
            info!("SOAP not available, skipping: {}", e);
        }
    }

    let run_id = lineage.start_run(
        "top_salesman_country",
        "kata-data",
        "Aggregates total sales per salesman across the entire country, ranked nationally",
        input_datasets,
    );

    let pipeline = DataPipeline::new().await?;
    pipeline.load_records(&all_records).await?;
    let results = pipeline.run_top_salesman_country().await?;
    let result_count = results.len() as u64;

    sink::sink_top_salesman_country(ANALYTICS_DB, &results)?;
    sink::record_pipeline_run_db(
        ANALYTICS_DB,
        &run_id,
        "top_salesman_country",
        "success",
        None,
        Some(total_ingested),
        Some(result_count),
    )?;

    let duration_ms = timer.finish(true);
    record_pipeline_run("top_salesman_country", "success");

    let mut facets = HashMap::new();
    facets.insert("total_input_records".into(), serde_json::json!(total_ingested));
    lineage.complete_run(
        &run_id,
        vec![sqlite_sink_dataset("top_salesman_country", result_count)],
        duration_ms,
        facets,
    );

    info!("✓ Pipeline top_salesman_country: {} salesmen ranked", result_count);
    Ok(())
}
