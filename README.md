# kafka-connect-solr

A production-grade Apache Solr sink connector for Kafka Connect.
The shape and configuration surface mirror the Confluent
[Elasticsearch sink connector](https://docs.confluent.io/kafka-connectors/elasticsearch/current/overview.html)
so moving a sink config from Elasticsearch to Solr is a drop-in change.

Performance: the connector uses SolrJ's `Http2SolrClient` so multiple
in-flight requests share a single connection, and an internal executor
pool drives `max.in.flight.requests` (default 8 vs Confluent ES default 5)
concurrent batches per task. This typically beats the Elasticsearch
connector on the same hardware on commit-bound workloads.

Package: `com.una.kafka.connect.solr`

## Architecture

```
+--------------------+      +----------------------+      +--------+
|  SolrSinkConnector |----->|     SolrSinkTask     |----->|  Solr  |
+--------------------+      +----------+-----------+      +--------+
                                       |
                                       v
                            +----------+-----------+
                            |     SolrWriter       |
                            +----------+-----------+
                                       |
        +---------------+---------------+----------------+
        v               v               v                v
SolrRecordConverter  SolrBulkProcessor  SolrSchemaManager  CollectionResolver
```

Helpers: `SolrClientFactory`, `SolrVersionDetector`, `RetryUtil`,
`transforms.WKBToLatLon` (PostGIS WKB -> Solr `LatLonPointSpatialField`).

## Configuration (mirrors the Elasticsearch sink)

| Key                          | Default     | Notes                                                              |
|------------------------------|-------------|--------------------------------------------------------------------|
| `solr.url`                   | _required*_ | Comma-separated base URLs (`http://host:8983/solr`)                |
| `solr.zk.host`               | _required*_ | Zookeeper string for SolrCloud (mutually exclusive with solr.url)  |
| `solr.collection`            | `""`        | Default collection / core. Falls back to topic if empty            |
| `connection.username`        | `""`        | Basic-auth user                                                    |
| `connection.password`        | `""`        | Basic-auth password                                                |
| `connection.timeout.ms`      | `5000`      | HTTP connect timeout                                               |
| `read.timeout.ms`            | `60000`     | HTTP read timeout                                                  |
| `key.ignore`                 | `false`     |                                                                    |
| `schema.ignore`              | `false`     |                                                                    |
| `behavior.on.null.values`    | `ignore`    | `ignore` / `delete` / `fail`                                       |
| `behavior.on.malformed.documents` | `fail` | `ignore` / `warn` / `fail`                                         |
| `batch.size`                 | `2000`      | Records per bulk update                                            |
| `linger.ms`                  | `50`        | Time to wait while filling a batch                                 |
| `flush.timeout.ms`           | `30000`     | Max wait when flushing                                             |
| `max.in.flight.requests`     | `8`         | Concurrent Solr requests per task                                  |
| `max.buffered.records`       | `20000`     | Back-pressure threshold                                            |
| `max.retries`                | `5`         | Max attempts on retryable failures                                 |
| `retry.backoff.ms`           | `200`       | Initial backoff, doubles up to 30s                                 |
| `write.method`               | `INDEX`     | `INDEX` / `UPSERT` / `ATOMIC_UPDATE`                               |
| `id.strategy`                | `KAFKA_KEY` | `KAFKA_KEY` / `RECORD_FIELD` / `TOPIC_PARTITION_OFFSET` / `UUID`   |
| `id.field`                   | `id`        | Dot-path for `RECORD_FIELD`                                        |
| `collection.naming.strategy` | `TOPIC`     | `TOPIC` / `STATIC` / `TOPIC_REGEX`                                 |
| `schema.auto.create`         | `false`     | Try to create the collection on SolrCloud                          |
| `schema.auto.evolve`         | `false`     | Auto-add new fields to managed schema                              |
| `commit.within.ms`           | `1000`      | Solr `commitWithin` sent with every batch                          |

\* exactly one of `solr.url` / `solr.zk.host` must be set.

## Delivery guarantees

- At-least-once delivery, with idempotent writes when the unique key is
  stable (`id.strategy` != `UUID`).
- `429` and `5xx` are retried with exponential backoff (max 30 s).
- `4xx` is reported to Kafka Connect so the record lands in the DLQ.
- Leader election / collection unavailable on SolrCloud retries
  transparently via the CloudHttp2SolrClient.

## Quick start (standalone Solr)

```bash
mvn -DskipTests package
docker compose up -d
curl -s -XPOST -H "Content-Type: application/json" \
    --data @config/quickstart-solr.properties \
    http://localhost:8084/connectors
```

## Building

```bash
mvn -B clean package        # builds target/kafka-connect-solr-*.jar + assembly zip
mvn -B test                 # unit tests
mvn -B verify               # Testcontainers IT against real Solr
```
