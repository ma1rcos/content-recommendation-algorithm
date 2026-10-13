//! Métricas Prometheus. Labels só com valores de cardinalidade limitada
//! (rota como padrão `/v1/feed/{user_id}`, nunca o ID concreto).

use prometheus::{
    Encoder, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts,
    Registry, TextEncoder,
};
use std::sync::LazyLock;

pub static REGISTRY: LazyLock<Registry> = LazyLock::new(Registry::new);

fn counter_vec(name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
    let c = IntCounterVec::new(Opts::new(name, help), labels).expect("métrica válida");
    REGISTRY
        .register(Box::new(c.clone()))
        .expect("registro único");
    c
}

fn histogram_vec(name: &str, help: &str, labels: &[&str], buckets: Vec<f64>) -> HistogramVec {
    let h = HistogramVec::new(HistogramOpts::new(name, help).buckets(buckets), labels)
        .expect("métrica válida");
    REGISTRY
        .register(Box::new(h.clone()))
        .expect("registro único");
    h
}

fn histogram(name: &str, help: &str, buckets: Vec<f64>) -> Histogram {
    let h = Histogram::with_opts(HistogramOpts::new(name, help).buckets(buckets))
        .expect("métrica válida");
    REGISTRY
        .register(Box::new(h.clone()))
        .expect("registro único");
    h
}

const LATENCY: [f64; 14] = [
    0.0005, 0.001, 0.0025, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
];

pub static HTTP_REQUESTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter_vec(
        "http_requests_total",
        "Requisições HTTP",
        &["method", "route", "status"],
    )
});
pub static HTTP_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    histogram_vec(
        "http_request_duration_seconds",
        "Latência HTTP",
        &["method", "route"],
        LATENCY.to_vec(),
    )
});
pub static FEED_SOURCE: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter_vec(
        "reco_feed_responses_total",
        "Respostas de feed por origem dos itens",
        &["source"],
    )
});
pub static FEED_CACHE: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter_vec(
        "reco_feed_cache_total",
        "Consultas ao feed pré-calculado",
        &["result"],
    )
});
pub static ITEMS_SERVED: LazyLock<IntCounter> = LazyLock::new(|| {
    let c = IntCounter::new("reco_feed_items_served_total", "Itens de feed entregues").unwrap();
    REGISTRY.register(Box::new(c.clone())).unwrap();
    c
});
pub static FILTERED: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter_vec(
        "reco_feed_filtered_total",
        "Itens removidos na entrega por motivo",
        &["reason"],
    )
});
pub static PAGE_AUTHORS: LazyLock<Histogram> = LazyLock::new(|| {
    histogram(
        "reco_feed_page_distinct_authors",
        "Autores distintos por página",
        vec![1.0, 2.0, 3.0, 5.0, 8.0, 10.0, 15.0, 20.0, 30.0, 50.0],
    )
});
pub static PAGE_TOP_AUTHOR_SHARE: LazyLock<Histogram> = LazyLock::new(|| {
    histogram(
        "reco_feed_page_top_author_share",
        "Fração da página ocupada pelo autor mais frequente",
        vec![0.05, 0.1, 0.15, 0.2, 0.3, 0.5, 0.75, 1.0],
    )
});
pub static ENGINE_CALLS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter_vec(
        "reco_engine_calls_total",
        "Chamadas síncronas ao motor Python",
        &["result"],
    )
});
pub static ENGINE_LATENCY: LazyLock<Histogram> = LazyLock::new(|| {
    histogram(
        "reco_engine_call_duration_seconds",
        "Latência das chamadas ao motor",
        LATENCY.to_vec(),
    )
});
pub static BREAKER_OPEN: LazyLock<IntGauge> = LazyLock::new(|| {
    let g = IntGauge::new(
        "reco_engine_breaker_open",
        "1 quando o circuit breaker do motor está aberto",
    )
    .unwrap();
    REGISTRY.register(Box::new(g.clone())).unwrap();
    g
});
pub static EVENTS_INGESTED: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter_vec(
        "reco_events_ingested_total",
        "Eventos enfileirados no stream",
        &["type"],
    )
});
pub static REBUILD_REQUESTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter_vec(
        "reco_rebuild_requests_total",
        "Pedidos de regeneração de feed",
        &["trigger", "result"],
    )
});
pub static DEPENDENCY_ERRORS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter_vec(
        "reco_dependency_errors_total",
        "Erros de dependências",
        &["dependency"],
    )
});

pub fn dependency_error(dep: &str) {
    DEPENDENCY_ERRORS.with_label_values(&[dep]).inc();
}

/// Força o registro de todas as métricas no início (aparecem zeradas em /metrics).
pub fn init() {
    LazyLock::force(&HTTP_REQUESTS);
    LazyLock::force(&HTTP_LATENCY);
    LazyLock::force(&FEED_SOURCE);
    LazyLock::force(&FEED_CACHE);
    LazyLock::force(&ITEMS_SERVED);
    LazyLock::force(&FILTERED);
    LazyLock::force(&PAGE_AUTHORS);
    LazyLock::force(&PAGE_TOP_AUTHOR_SHARE);
    LazyLock::force(&ENGINE_CALLS);
    LazyLock::force(&ENGINE_LATENCY);
    LazyLock::force(&BREAKER_OPEN);
    LazyLock::force(&EVENTS_INGESTED);
    LazyLock::force(&REBUILD_REQUESTS);
    LazyLock::force(&DEPENDENCY_ERRORS);
}

pub fn render() -> String {
    let mut buf = Vec::new();
    TextEncoder::new().encode(&REGISTRY.gather(), &mut buf).ok();
    String::from_utf8(buf).unwrap_or_default()
}
