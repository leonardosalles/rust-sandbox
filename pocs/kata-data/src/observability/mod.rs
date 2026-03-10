use prometheus::{
    register_counter_vec, register_gauge, register_histogram_vec, CounterVec, Gauge, HistogramVec,
    TextEncoder, Encoder,
};
use std::sync::OnceLock;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

static PIPELINE_RUNS_TOTAL: OnceLock<CounterVec> = OnceLock::new();
static PIPELINE_DURATION_SECONDS: OnceLock<HistogramVec> = OnceLock::new();
static RECORDS_PROCESSED_TOTAL: OnceLock<CounterVec> = OnceLock::new();
static INGESTION_ERRORS_TOTAL: OnceLock<CounterVec> = OnceLock::new();
static ACTIVE_PIPELINES: OnceLock<Gauge> = OnceLock::new();

pub fn init_metrics() {
    PIPELINE_RUNS_TOTAL.get_or_init(|| {
        register_counter_vec!(
            "kata_pipeline_runs_total",
            "Total number of pipeline runs",
            &["pipeline", "status"]
        )
        .unwrap()
    });

    PIPELINE_DURATION_SECONDS.get_or_init(|| {
        register_histogram_vec!(
            "kata_pipeline_duration_seconds",
            "Pipeline execution duration in seconds",
            &["pipeline"],
            vec![0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0]
        )
        .unwrap()
    });

    RECORDS_PROCESSED_TOTAL.get_or_init(|| {
        register_counter_vec!(
            "kata_records_processed_total",
            "Total records processed per source",
            &["source", "pipeline"]
        )
        .unwrap()
    });

    INGESTION_ERRORS_TOTAL.get_or_init(|| {
        register_counter_vec!(
            "kata_ingestion_errors_total",
            "Total ingestion errors per source",
            &["source"]
        )
        .unwrap()
    });

    ACTIVE_PIPELINES.get_or_init(|| {
        register_gauge!(
            "kata_active_pipelines",
            "Number of currently running pipelines"
        )
        .unwrap()
    });
}

pub fn record_pipeline_run(pipeline: &str, status: &str) {
    if let Some(counter) = PIPELINE_RUNS_TOTAL.get() {
        counter.with_label_values(&[pipeline, status]).inc();
    }
}

pub fn record_pipeline_duration(pipeline: &str, duration_secs: f64) {
    if let Some(hist) = PIPELINE_DURATION_SECONDS.get() {
        hist.with_label_values(&[pipeline]).observe(duration_secs);
    }
}

pub fn record_records_processed(source: &str, pipeline: &str, count: u64) {
    if let Some(counter) = RECORDS_PROCESSED_TOTAL.get() {
        counter.with_label_values(&[source, pipeline]).inc_by(count as f64);
    }
}

pub fn record_ingestion_error(source: &str) {
    if let Some(counter) = INGESTION_ERRORS_TOTAL.get() {
        counter.with_label_values(&[source]).inc();
    }
}

pub fn set_active_pipelines(count: f64) {
    if let Some(gauge) = ACTIVE_PIPELINES.get() {
        gauge.set(count);
    }
}

#[allow(dead_code)]
pub fn gather_metrics() -> String {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap_or_default();
    String::from_utf8(buffer).unwrap_or_default()
}

pub fn init_tracing() {
    let otel_layer = tracing_opentelemetry::layer();

    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_target(true)
                .with_thread_ids(true)
                .with_span_list(true),
        )
        .with(otel_layer)
        .init();
}

pub struct PipelineTimer {
    pipeline_name: String,
    start: std::time::Instant,
}

impl PipelineTimer {
    pub fn start(pipeline_name: &str) -> Self {
        set_active_pipelines(1.0);
        tracing::info!(pipeline = %pipeline_name, "Pipeline timer started");
        Self {
            pipeline_name: pipeline_name.to_string(),
            start: std::time::Instant::now(),
        }
    }

    pub fn finish(self, success: bool) -> u64 {
        let duration = self.start.elapsed();
        let duration_secs = duration.as_secs_f64();
        let duration_ms = duration.as_millis() as u64;
        let status = if success { "success" } else { "error" };

        record_pipeline_run(&self.pipeline_name, status);
        record_pipeline_duration(&self.pipeline_name, duration_secs);
        set_active_pipelines(0.0);

        tracing::info!(
            pipeline = %self.pipeline_name,
            duration_ms = %duration_ms,
            status = %status,
            "Pipeline finished"
        );
        duration_ms
    }
}
