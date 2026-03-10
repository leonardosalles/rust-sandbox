use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DatasetType {
    Source,
    Intermediate,
    Sink,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetFacet {
    pub name: String,
    pub namespace: String,
    pub dataset_type: DatasetType,
    pub schema: Vec<FieldSchema>,
    pub row_count: Option<u64>,
    pub source: String, // can be "sqlite", "csv", "soap"
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldSchema {
    pub name: String,
    pub field_type: String,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobFacet {
    pub name: String,
    pub namespace: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RunState {
    Start,
    Running,
    Complete,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageEvent {
    pub event_type: RunState,
    pub event_time: DateTime<Utc>,
    pub run_id: String,
    pub job: JobFacet,
    pub inputs: Vec<DatasetFacet>,
    pub outputs: Vec<DatasetFacet>,
    pub duration_ms: Option<u64>,
    pub error: Option<String>,
    pub custom_facets: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct LineageTracker {
    events: Arc<Mutex<Vec<LineageEvent>>>,
}

impl LineageTracker {
    pub fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn start_run(
        &self,
        job_name: &str,
        job_namespace: &str,
        description: &str,
        inputs: Vec<DatasetFacet>,
    ) -> String {
        let run_id = Uuid::new_v4().to_string();
        let event = LineageEvent {
            event_type: RunState::Start,
            event_time: Utc::now(),
            run_id: run_id.clone(),
            job: JobFacet {
                name: job_name.to_string(),
                namespace: job_namespace.to_string(),
                description: description.to_string(),
            },
            inputs,
            outputs: vec![],
            duration_ms: None,
            error: None,
            custom_facets: HashMap::new(),
        };
        self.events.lock().unwrap().push(event);
        tracing::info!(run_id = %run_id, job = %job_name, "Lineage: pipeline run STARTED");
        run_id
    }

    pub fn complete_run(
        &self,
        run_id: &str,
        outputs: Vec<DatasetFacet>,
        duration_ms: u64,
        custom_facets: HashMap<String, serde_json::Value>,
    ) {
        let events = self.events.lock().unwrap();
        let job = events
            .iter()
            .find(|e| e.run_id == run_id)
            .map(|e| e.job.clone())
            .unwrap_or(JobFacet {
                name: "unknown".into(),
                namespace: "kata-data".into(),
                description: "".into(),
            });
        drop(events);

        let event = LineageEvent {
            event_type: RunState::Complete,
            event_time: Utc::now(),
            run_id: run_id.to_string(),
            job: job.clone(),
            inputs: vec![],
            outputs,
            duration_ms: Some(duration_ms),
            error: None,
            custom_facets,
        };
        self.events.lock().unwrap().push(event);
        tracing::info!(run_id = %run_id, job = %job.name, duration_ms = %duration_ms, "Lineage: pipeline run COMPLETE");
    }

    #[allow(dead_code)]
    pub fn fail_run(&self, run_id: &str, error: &str) {
        let event = LineageEvent {
            event_type: RunState::Failed,
            event_time: Utc::now(),
            run_id: run_id.to_string(),
            job: JobFacet {
                name: "unknown".into(),
                namespace: "kata-data".into(),
                description: "".into(),
            },
            inputs: vec![],
            outputs: vec![],
            duration_ms: None,
            error: Some(error.to_string()),
            custom_facets: HashMap::new(),
        };
        self.events.lock().unwrap().push(event);
        tracing::error!(run_id = %run_id, error = %error, "Lineage: pipeline run FAILED");
    }

    #[allow(dead_code)]
    #[allow(dead_code)]
    pub fn get_events(&self) -> Vec<LineageEvent> {
        self.events.lock().unwrap().clone()
    }

    pub fn get_lineage_graph(&self) -> LineageGraph {
        let events = self.events.lock().unwrap();
        let mut nodes: HashMap<String, LineageNode> = HashMap::new();
        let mut edges: Vec<LineageEdge> = Vec::new();

        for event in events.iter() {
            if event.event_type == RunState::Start {
                for input in &event.inputs {
                    let key = format!("{}:{}", input.namespace, input.name);
                    nodes.entry(key.clone()).or_insert(LineageNode {
                        id: key.clone(),
                        name: input.name.clone(),
                        node_type: "dataset".into(),
                        source: input.source.clone(),
                    });
                    edges.push(LineageEdge {
                        from: key,
                        to: format!("job:{}", event.job.name),
                        label: "reads".into(),
                        run_id: event.run_id.clone(),
                    });
                }
                nodes
                    .entry(format!("job:{}", event.job.name))
                    .or_insert(LineageNode {
                        id: format!("job:{}", event.job.name),
                        name: event.job.name.clone(),
                        node_type: "job".into(),
                        source: event.job.namespace.clone(),
                    });
            }

            if event.event_type == RunState::Complete {
                for output in &event.outputs {
                    let key = format!("{}:{}", output.namespace, output.name);
                    nodes.entry(key.clone()).or_insert(LineageNode {
                        id: key.clone(),
                        name: output.name.clone(),
                        node_type: "dataset".into(),
                        source: output.source.clone(),
                    });
                    edges.push(LineageEdge {
                        from: format!("job:{}", event.job.name),
                        to: key,
                        label: "writes".into(),
                        run_id: event.run_id.clone(),
                    });
                }
            }
        }

        LineageGraph {
            nodes: nodes.into_values().collect(),
            edges,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageNode {
    pub id: String,
    pub name: String,
    pub node_type: String, // "dataset" | "job"
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageEdge {
    pub from: String,
    pub to: String,
    pub label: String,
    pub run_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageGraph {
    pub nodes: Vec<LineageNode>,
    pub edges: Vec<LineageEdge>,
}

pub fn sqlite_source_dataset(table: &str, row_count: u64) -> DatasetFacet {
    DatasetFacet {
        name: table.to_string(),
        namespace: "sqlite://data/sales.db".into(),
        dataset_type: DatasetType::Source,
        schema: vec![
            FieldSchema { name: "id".into(), field_type: "INTEGER".into(), nullable: false },
            FieldSchema { name: "city".into(), field_type: "TEXT".into(), nullable: false },
            FieldSchema { name: "salesman_id".into(), field_type: "TEXT".into(), nullable: false },
            FieldSchema { name: "amount".into(), field_type: "REAL".into(), nullable: false },
            FieldSchema { name: "timestamp".into(), field_type: "TEXT".into(), nullable: false },
        ],
        row_count: Some(row_count),
        source: "sqlite".into(),
    }
}

pub fn csv_source_dataset(filename: &str, row_count: u64) -> DatasetFacet {
    DatasetFacet {
        name: filename.to_string(),
        namespace: "file://data/".into(),
        dataset_type: DatasetType::Source,
        schema: vec![
            FieldSchema { name: "sale_id".into(), field_type: "STRING".into(), nullable: false },
            FieldSchema { name: "city".into(), field_type: "STRING".into(), nullable: false },
            FieldSchema { name: "salesman_id".into(), field_type: "STRING".into(), nullable: false },
            FieldSchema { name: "amount".into(), field_type: "FLOAT".into(), nullable: false },
        ],
        row_count: Some(row_count),
        source: "csv".into(),
    }
}

pub fn soap_source_dataset(row_count: u64) -> DatasetFacet {
    DatasetFacet {
        name: "soap_sales".into(),
        namespace: "http://localhost:8081/soap/sales".into(),
        dataset_type: DatasetType::Source,
        schema: vec![
            FieldSchema { name: "SaleId".into(), field_type: "STRING".into(), nullable: false },
            FieldSchema { name: "City".into(), field_type: "STRING".into(), nullable: false },
            FieldSchema { name: "SalesmanId".into(), field_type: "STRING".into(), nullable: false },
            FieldSchema { name: "Amount".into(), field_type: "FLOAT".into(), nullable: false },
        ],
        row_count: Some(row_count),
        source: "soap_ws".into(),
    }
}

pub fn sqlite_sink_dataset(table: &str, row_count: u64) -> DatasetFacet {
    DatasetFacet {
        name: table.to_string(),
        namespace: "sqlite://data/analytics.db".into(),
        dataset_type: DatasetType::Sink,
        schema: vec![],
        row_count: Some(row_count),
        source: "sqlite".into(),
    }
}
