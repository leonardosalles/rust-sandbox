use anyhow::Result;
use rusqlite::Connection;
use tracing::{info, instrument};

use crate::processing::{TopSalesPerCity, TopSalesmanCountry};

pub fn init_analytics_db(db_path: &str) -> Result<()> {
    let conn = Connection::open(db_path)?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS top_sales_per_city (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            city                TEXT NOT NULL,
            state               TEXT NOT NULL,
            total_sales         REAL NOT NULL,
            transaction_count   INTEGER NOT NULL,
            rank                INTEGER NOT NULL,
            computed_at         TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS top_salesman_country (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            salesman_id         TEXT NOT NULL,
            salesman_name       TEXT NOT NULL,
            total_sales         REAL NOT NULL,
            transaction_count   INTEGER NOT NULL,
            rank                INTEGER NOT NULL,
            cities_served       INTEGER NOT NULL,
            computed_at         TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id      TEXT NOT NULL,
            pipeline    TEXT NOT NULL,
            status      TEXT NOT NULL,
            duration_ms INTEGER,
            records_in  INTEGER,
            records_out INTEGER,
            ran_at      TEXT NOT NULL
        );",
    )?;
    info!(db = %db_path, "Analytics DB initialized");
    Ok(())
}

#[instrument(name = "sink_top_sales_per_city", skip(results))]
pub fn sink_top_sales_per_city(db_path: &str, results: &[TopSalesPerCity]) -> Result<u64> {
    let conn = Connection::open(db_path)?;
    let computed_at = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

    conn.execute("DELETE FROM top_sales_per_city", [])?;
    for r in results {
        conn.execute(
            "INSERT INTO top_sales_per_city (city, state, total_sales, transaction_count, rank, computed_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![r.city, r.state, r.total_sales, r.transaction_count, r.rank, computed_at],
        )?;
    }
    let count = results.len() as u64;
    info!(records = %count, table = "top_sales_per_city", "Sink: wrote aggregated results");
    Ok(count)
}

#[instrument(name = "sink_top_salesman_country", skip(results))]
pub fn sink_top_salesman_country(db_path: &str, results: &[TopSalesmanCountry]) -> Result<u64> {
    let conn = Connection::open(db_path)?;
    let computed_at = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

    conn.execute("DELETE FROM top_salesman_country", [])?;
    for r in results {
        conn.execute(
            "INSERT INTO top_salesman_country
                (salesman_id, salesman_name, total_sales, transaction_count, rank, cities_served, computed_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![
                r.salesman_id, r.salesman_name, r.total_sales,
                r.transaction_count, r.rank, r.cities_served, computed_at
            ],
        )?;
    }
    let count = results.len() as u64;
    info!(records = %count, table = "top_salesman_country", "Sink: wrote aggregated results");
    Ok(count)
}

pub fn record_pipeline_run_db(
    db_path: &str,
    run_id: &str,
    pipeline: &str,
    status: &str,
    duration_ms: Option<u64>,
    records_in: Option<u64>,
    records_out: Option<u64>,
) -> Result<()> {
    let conn = Connection::open(db_path)?;
    let ran_at = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    conn.execute(
        "INSERT INTO pipeline_runs (run_id, pipeline, status, duration_ms, records_in, records_out, ran_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        rusqlite::params![
            run_id, pipeline, status,
            duration_ms.map(|d| d as i64),
            records_in.map(|r| r as i64),
            records_out.map(|r| r as i64),
            ran_at
        ],
    )?;
    Ok(())
}
