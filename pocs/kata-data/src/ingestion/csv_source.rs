use anyhow::Result;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use tracing::{info, instrument, warn};

use super::sqlite_source::SaleRecord;

#[derive(Debug, Deserialize)]
struct CsvSaleRow {
    sale_id: String,
    city: String,
    state: String,
    salesman_id: String,
    salesman_name: String,
    amount: f64,
    quantity: i64,
    timestamp: String,
}

pub fn resolve_csv_path(explicit_path: Option<&str>) -> PathBuf {
    if let Some(p) = explicit_path {
        return PathBuf::from(p);
    }
    if let Ok(env_path) = std::env::var("KATA_CSV_PATH") {
        info!(path = %env_path, "CSV path resolved from KATA_CSV_PATH env var");
        return PathBuf::from(env_path);
    }
    let default = PathBuf::from("data/regional_sales.csv");
    info!(path = %default.display(), "CSV path using default: data/regional_sales.csv");
    default
}

#[instrument(name = "ingest_csv", skip_all, fields(path))]
pub fn ingest_from_csv(explicit_path: Option<&str>) -> Result<(Vec<SaleRecord>, u64)> {
    let path = resolve_csv_path(explicit_path);
    tracing::Span::current().record("path", path.display().to_string().as_str());

    if !Path::new(&path).exists() {
        warn!(
            path = %path.display(),
            "CSV file not found — skipping CSV source. \
             Set KATA_CSV_PATH or pass --csv-path to specify the file."
        );
        return Ok((vec![], 0));
    }

    let mut records = Vec::new();
    let mut rdr = csv::Reader::from_path(&path)?;

    for result in rdr.deserialize::<CsvSaleRow>() {
        match result {
            Ok(row) => records.push(SaleRecord {
                sale_id: row.sale_id,
                city: row.city,
                state: row.state,
                salesman_id: row.salesman_id,
                salesman_name: row.salesman_name,
                amount: row.amount,
                quantity: row.quantity,
                timestamp: row.timestamp,
                source: "csv_filesystem".into(),
            }),
            Err(e) => warn!(error = %e, "Skipping malformed CSV row"),
        }
    }

    let count = records.len() as u64;
    info!(source = "csv", records = %count, file = %path.display(), "Ingested records from CSV");
    Ok((records, count))
}
