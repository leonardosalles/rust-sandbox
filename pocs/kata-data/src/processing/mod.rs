pub mod sink;

use anyhow::Result;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};

use crate::ingestion::SaleRecord;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopSalesPerCity {
    pub city: String,
    pub state: String,
    pub total_sales: f64,
    pub transaction_count: i64,
    pub rank: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopSalesmanCountry {
    pub salesman_id: String,
    pub salesman_name: String,
    pub total_sales: f64,
    pub transaction_count: i64,
    pub rank: i64,
    pub cities_served: i64,
}

/// Core DataFusion processing engine
/// Registers all ingested records as an in-memory table and runs
/// SQL pipelines with DataFusion query engine
pub struct DataPipeline {
    ctx: SessionContext,
}

impl DataPipeline {
    pub async fn new() -> Result<Self> {
        let ctx = SessionContext::new();
        Ok(Self { ctx })
    }

    /// Loads all records from the 3 sources into DataFusion as a unified table
    #[instrument(name = "load_unified_table", skip(self, records))]
    pub async fn load_records(&self, records: &[SaleRecord]) -> Result<u64> {
        let count = records.len();

        // Build Arrow record batch from our SaleRecord structs
        let sale_ids: Vec<&str> = records.iter().map(|r| r.sale_id.as_str()).collect();
        let cities: Vec<&str> = records.iter().map(|r| r.city.as_str()).collect();
        let states: Vec<&str> = records.iter().map(|r| r.state.as_str()).collect();
        let salesman_ids: Vec<&str> = records.iter().map(|r| r.salesman_id.as_str()).collect();
        let salesman_names: Vec<&str> = records.iter().map(|r| r.salesman_name.as_str()).collect();
        let amounts: Vec<f64> = records.iter().map(|r| r.amount).collect();
        let quantities: Vec<i64> = records.iter().map(|r| r.quantity).collect();
        let sources: Vec<&str> = records.iter().map(|r| r.source.as_str()).collect();

        use datafusion::arrow::array::*;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("sale_id", DataType::Utf8, false),
            Field::new("city", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salesman_id", DataType::Utf8, false),
            Field::new("salesman_name", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("quantity", DataType::Int64, false),
            Field::new("source", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(sale_ids)),
                Arc::new(StringArray::from(cities)),
                Arc::new(StringArray::from(states)),
                Arc::new(StringArray::from(salesman_ids)),
                Arc::new(StringArray::from(salesman_names)),
                Arc::new(Float64Array::from(amounts)),
                Arc::new(Int64Array::from(quantities)),
                Arc::new(StringArray::from(sources)),
            ],
        )?;

        self.ctx
            .register_batch("unified_sales", batch)?;

        info!(records = %count, "Loaded unified sales table into DataFusion");
        Ok(count as u64)
    }

    /// Pipeline 1
    #[instrument(name = "pipeline_top_sales_per_city", skip(self))]
    pub async fn run_top_sales_per_city(&self) -> Result<Vec<TopSalesPerCity>> {
        let sql = r#"
            SELECT
                city,
                state,
                ROUND(SUM(amount), 2)    AS total_sales,
                COUNT(*)                  AS transaction_count,
                CAST(RANK() OVER (ORDER BY SUM(amount) DESC) AS BIGINT) AS rank
            FROM unified_sales
            GROUP BY city, state
            ORDER BY total_sales DESC
        "#;

        info!("Running Pipeline: Top Sales per City");
        let df = self.ctx.sql(sql).await?;
        let batches = df.collect().await?;

        let mut results = Vec::new();
        for batch in &batches {
            use datafusion::arrow::array::*;
            let city_col = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            let state_col = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            let sales_col = batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
            let count_col = batch.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
            let rank_col = batch.column(4).as_any().downcast_ref::<Int64Array>().unwrap();

            for i in 0..batch.num_rows() {
                results.push(TopSalesPerCity {
                    city: city_col.value(i).to_string(),
                    state: state_col.value(i).to_string(),
                    total_sales: sales_col.value(i),
                    transaction_count: count_col.value(i),
                    rank: rank_col.value(i),
                });
            }
        }

        info!(results = %results.len(), "Pipeline Top Sales per City completed");
        Ok(results)
    }

    /// Pipeline 2
    #[instrument(name = "pipeline_top_salesman_country", skip(self))]
    pub async fn run_top_salesman_country(&self) -> Result<Vec<TopSalesmanCountry>> {
        let sql = r#"
            SELECT
                salesman_id,
                salesman_name,
                ROUND(SUM(amount), 2)         AS total_sales,
                COUNT(*)                       AS transaction_count,
                CAST(RANK() OVER (ORDER BY SUM(amount) DESC) AS BIGINT) AS rank,
                CAST(COUNT(DISTINCT city) AS BIGINT)           AS cities_served
            FROM unified_sales
            GROUP BY salesman_id, salesman_name
            ORDER BY total_sales DESC
        "#;

        info!("Running Pipeline: Top Salesman Country");
        let df = self.ctx.sql(sql).await?;
        let batches = df.collect().await?;

        let mut results = Vec::new();
        for batch in &batches {
            use datafusion::arrow::array::*;
            let id_col = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            let name_col = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            let sales_col = batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
            let count_col = batch.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
            let rank_col = batch.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
            let cities_col = batch.column(5).as_any().downcast_ref::<Int64Array>().unwrap();

            for i in 0..batch.num_rows() {
                results.push(TopSalesmanCountry {
                    salesman_id: id_col.value(i).to_string(),
                    salesman_name: name_col.value(i).to_string(),
                    total_sales: sales_col.value(i),
                    transaction_count: count_col.value(i),
                    rank: rank_col.value(i),
                    cities_served: cities_col.value(i),
                });
            }
        }

        info!(results = %results.len(), "Pipeline Top Salesman Country completed");
        Ok(results)
    }
}
