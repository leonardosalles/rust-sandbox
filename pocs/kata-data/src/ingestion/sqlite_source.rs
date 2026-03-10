use anyhow::Result;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SaleRecord {
    pub sale_id: String,
    pub city: String,
    pub state: String,
    pub salesman_id: String,
    pub salesman_name: String,
    pub amount: f64,
    pub quantity: i64,
    pub timestamp: String,
    pub source: String,
}

pub fn seed_source_db(db_path: &str) -> Result<u64> {
    let conn = Connection::open(db_path)?;

    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sales (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            sale_id     TEXT NOT NULL,
            city        TEXT NOT NULL,
            state       TEXT NOT NULL,
            salesman_id TEXT NOT NULL,
            salesman_name TEXT NOT NULL,
            amount      REAL NOT NULL,
            quantity    INTEGER NOT NULL,
            timestamp   TEXT NOT NULL,
            source      TEXT DEFAULT 'sqlite_db'
        );
        CREATE TABLE IF NOT EXISTS salesmen (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            salesman_id TEXT UNIQUE NOT NULL,
            name        TEXT NOT NULL,
            city        TEXT NOT NULL,
            state       TEXT NOT NULL,
            region      TEXT NOT NULL
        );",
    )?;

    // Check if already seeded
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM sales", [], |r| r.get(0))?;
    if count > 0 {
        info!("Source DB already seeded with {} records", count);
        return Ok(count as u64);
    }

    let salesmen = vec![
        ("SM0001", "João Silva", "São Paulo", "SP", "Sudeste"),
        ("SM0002", "Maria Santos", "Rio de Janeiro", "RJ", "Sudeste"),
        ("SM0003", "Carlos Oliveira", "Belo Horizonte", "MG", "Sudeste"),
        ("SM0004", "Ana Lima", "Curitiba", "PR", "Sul"),
        ("SM0005", "Pedro Souza", "Porto Alegre", "RS", "Sul"),
        ("SM0006", "Lucia Costa", "Salvador", "BA", "Nordeste"),
        ("SM0007", "Roberto Ferreira", "Recife", "PE", "Nordeste"),
        ("SM0008", "Fernanda Alves", "Fortaleza", "CE", "Nordeste"),
        ("SM0009", "Bruno Martins", "Manaus", "AM", "Norte"),
        ("SM0010", "Camila Rocha", "Belém", "PA", "Norte"),
    ];

    for (id, name, city, state, region) in &salesmen {
        conn.execute(
            "INSERT OR IGNORE INTO salesmen (salesman_id, name, city, state, region) VALUES (?1,?2,?3,?4,?5)",
            rusqlite::params![id, name, city, state, region],
        )?;
    }

    // Generate sales records
    let sales_data = vec![
        ("SP0001", "São Paulo", "SP", "SM0001", "João Silva", 45000.0, 90),
        ("SP0002", "São Paulo", "SP", "SM0001", "João Silva", 32000.0, 64),
        ("SP0003", "São Paulo", "SP", "SM0002", "Maria Santos", 28000.0, 56),
        ("RJ0001", "Rio de Janeiro", "RJ", "SM0002", "Maria Santos", 67000.0, 134),
        ("RJ0002", "Rio de Janeiro", "RJ", "SM0003", "Carlos Oliveira", 41000.0, 82),
        ("MG0001", "Belo Horizonte", "MG", "SM0003", "Carlos Oliveira", 55000.0, 110),
        ("MG0002", "Belo Horizonte", "MG", "SM0004", "Ana Lima", 23000.0, 46),
        ("PR0001", "Curitiba", "PR", "SM0004", "Ana Lima", 38000.0, 76),
        ("PR0002", "Curitiba", "PR", "SM0005", "Pedro Souza", 29000.0, 58),
        ("RS0001", "Porto Alegre", "RS", "SM0005", "Pedro Souza", 52000.0, 104),
        ("RS0002", "Porto Alegre", "RS", "SM0006", "Lucia Costa", 17000.0, 34),
        ("BA0001", "Salvador", "BA", "SM0006", "Lucia Costa", 44000.0, 88),
        ("BA0002", "Salvador", "BA", "SM0007", "Roberto Ferreira", 31000.0, 62),
        ("PE0001", "Recife", "PE", "SM0007", "Roberto Ferreira", 26000.0, 52),
        ("CE0001", "Fortaleza", "CE", "SM0008", "Fernanda Alves", 33000.0, 66),
        ("AM0001", "Manaus", "AM", "SM0009", "Bruno Martins", 19000.0, 38),
        ("PA0001", "Belém", "PA", "SM0010", "Camila Rocha", 22000.0, 44),
        ("SP0004", "São Paulo", "SP", "SM0001", "João Silva", 71000.0, 142),
        ("RJ0003", "Rio de Janeiro", "RJ", "SM0002", "Maria Santos", 88000.0, 176),
        ("MG0003", "Belo Horizonte", "MG", "SM0003", "Carlos Oliveira", 61000.0, 122),
    ];

    let mut inserted = 0u64;
    let timestamp = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    for (sale_id, city, state, salesman_id, salesman_name, amount, qty) in &sales_data {
        conn.execute(
            "INSERT INTO sales (sale_id, city, state, salesman_id, salesman_name, amount, quantity, timestamp, source)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,'sqlite_db')",
            rusqlite::params![sale_id, city, state, salesman_id, salesman_name, amount, qty, timestamp],
        )?;
        inserted += 1;
    }

    info!("Seeded source DB with {} sales records", inserted);
    Ok(inserted)
}

#[instrument(name = "ingest_sqlite", skip_all)]
pub fn ingest_from_sqlite(db_path: &str) -> Result<(Vec<SaleRecord>, u64)> {
    let conn = Connection::open(db_path)?;
    let mut stmt = conn.prepare(
        "SELECT sale_id, city, state, salesman_id, salesman_name, amount, quantity, timestamp, source FROM sales"
    )?;

    let records: Vec<SaleRecord> = stmt
        .query_map([], |row| {
            Ok(SaleRecord {
                sale_id: row.get(0)?,
                city: row.get(1)?,
                state: row.get(2)?,
                salesman_id: row.get(3)?,
                salesman_name: row.get(4)?,
                amount: row.get(5)?,
                quantity: row.get(6)?,
                timestamp: row.get(7)?,
                source: row.get(8)?,
            })
        })?
        .filter_map(|r| r.ok())
        .collect();

    let count = records.len() as u64;
    info!(source = "sqlite", records = %count, "Ingested records from SQLite DB");
    Ok((records, count))
}
