# 🌊 Streaming Data Pipeline — GCP

[![Throughput](https://img.shields.io/badge/Throughput-1M%2B%20events%2Fsec-blue)](.)
[![Latency](https://img.shields.io/badge/End--to--end-< 800ms-green)](.)
[![Uptime](https://img.shields.io/badge/Uptime-99.99%25-orange)](.)

> Production streaming pipeline processing **1M+ events/second** with end-to-end latency under 800ms. Exactly-once processing guarantees, auto-scaling and full observability via Cloud Monitoring.

## 🏆 Production Stats
- **1M+ events/second** at peak (Black Friday tested: 2.3M/s)
- **< 800ms** end-to-end latency (P99: 1.2s)
- **99.99% uptime** (26 minutes downtime in 18 months)
- **Zero data loss** — exactly-once semantics with Dataflow

## 🏗️ Pipeline Architecture
```
Event Sources ──▶ Pub/Sub ──▶ Dataflow (Beam) ──▶ BigQuery (batch)
                              │  • Parsing          + Redis (real-time)
                              │  • Enrichment        + Pub/Sub (downstream)
                              │  • Aggregation
                              ▼
                         Dead Letter Queue ──▶ Error Handling + Replay
```
