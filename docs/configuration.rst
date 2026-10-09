Connector
^^^^^^^^^

``solr.url``
  Comma-separated Solr base URLs (e.g. http://solr:8983/solr). One of solr.url or solr.zk.host is required.

  * Type: list
  * Default: ""
  * Importance: high

``solr.zk.host``
  Zookeeper connection string for SolrCloud (e.g. zk1:2181,zk2:2181/solr).

  * Type: string
  * Default: ""
  * Importance: high

``solr.collection``
  Default Solr collection / core. Falls back to topic name when empty.

  * Type: string
  * Default: ""
  * Importance: high

``connection.username``
  Basic-auth username (optional).

  * Type: string
  * Default: ""
  * Importance: medium

``connection.password``
  Basic-auth password (optional).

  * Type: password
  * Default: [hidden]
  * Importance: medium

``connection.timeout.ms``
  HTTP connect timeout in ms.

  * Type: int
  * Default: 5000 (5 seconds)
  * Importance: low

``read.timeout.ms``
  HTTP socket read timeout in ms.

  * Type: int
  * Default: 60000 (1 minute)
  * Importance: low

``connection.compression``
  Enable response payload compression (Accept-Encoding header).

  * Type: boolean
  * Default: false
  * Importance: low

``connection.compression.algorithm``
  Algorithm advertised when connection.compression=true. GZIP | ZSTD | NONE. ZSTD requires Solr 9.1+.

  * Type: string
  * Default: GZIP
  * Importance: low

``connection.compression.requests``
  Gzip outbound request bodies. Worthwhile on WAN deploys where bandwidth is the limit; costs CPU on LANs where bandwidth is plentiful. Off by default.

  * Type: boolean
  * Default: false
  * Importance: low

Proxy
^^^^^

``proxy.host``
  HTTP proxy host. Leave empty for no proxy.

  * Type: string
  * Default: ""
  * Importance: low

``proxy.port``
  HTTP proxy port. 0 disables.

  * Type: int
  * Default: 0
  * Importance: low

``proxy.username``
  HTTP proxy basic-auth username.

  * Type: string
  * Default: ""
  * Importance: low

``proxy.password``
  HTTP proxy basic-auth password.

  * Type: password
  * Default: [hidden]
  * Importance: low

Security
^^^^^^^^

``solr.security.protocol``
  PLAINTEXT or SSL.

  * Type: string
  * Default: PLAINTEXT
  * Importance: medium

``ssl.keystore.location``
  Path to the client keystore (mTLS).

  * Type: string
  * Default: ""
  * Importance: medium

``ssl.keystore.password``
  Keystore password.

  * Type: password
  * Default: [hidden]
  * Importance: medium

``ssl.keystore.type``
  JKS, PKCS12 or BCFKS.

  * Type: string
  * Default: JKS
  * Importance: low

``ssl.key.password``
  Optional per-key password if different from the keystore password.

  * Type: password
  * Default: [hidden]
  * Importance: low

``ssl.truststore.location``
  Path to the truststore.

  * Type: string
  * Default: ""
  * Importance: medium

``ssl.truststore.password``
  Truststore password.

  * Type: password
  * Default: [hidden]
  * Importance: medium

``ssl.truststore.type``
  JKS, PKCS12 or BCFKS.

  * Type: string
  * Default: JKS
  * Importance: low

``ssl.protocol``
  Default SSL protocol.

  * Type: string
  * Default: TLSv1.3
  * Importance: low

Kerberos
^^^^^^^^

``kerberos.user.principal``
  Not supported: a non-empty value fails validation. Use connection.username / connection.password.

  * Type: string
  * Default: ""
  * Importance: low

``kerberos.keytab.path``
  Not supported: a non-empty value fails validation. Use connection.username / connection.password.

  * Type: string
  * Default: ""
  * Importance: low

Behavior
^^^^^^^^

``key.ignore``
  If true the connector ignores the record key when generating the doc id.

  * Type: boolean
  * Default: false
  * Importance: high

``schema.ignore``
  If true skip schema introspection.

  * Type: boolean
  * Default: false
  * Importance: medium

``topic.key.ignore``
  Comma-separated topic names where key.ignore is forced true.

  * Type: list
  * Default: ""
  * Importance: low

``topic.schema.ignore``
  Comma-separated topic names where schema.ignore is forced true.

  * Type: list
  * Default: ""
  * Importance: low

``compact.map.entries``
  true = flatten map fields with dotted-path keys. false = each entry of a nested map becomes <field>.key and <field>.value.

  * Type: boolean
  * Default: true
  * Importance: low

``drop.invalid.message``
  Alias for behavior.on.malformed.documents=ignore.

  * Type: boolean
  * Default: false
  * Importance: low

``behavior.on.null.values``
  Tombstones: ignore | delete | fail.

  * Type: string
  * Default: ignore
  * Importance: low

``behavior.on.malformed.documents``
  Malformed: ignore | warn | fail.

  * Type: string
  * Default: fail
  * Importance: low

Throughput
^^^^^^^^^^

``batch.size``
  Records per Solr bulk update.

  * Type: int
  * Default: 2000
  * Importance: high

``bulk.size.bytes``
  Byte size cap per bulk request. Whichever of batch.size or bulk.size.bytes hits first triggers a flush. Set 0 to disable byte-size capping.

  * Type: long
  * Default: 5242880 (5 mebibytes)
  * Importance: medium

``linger.ms``
  Max ms to wait while filling a batch before sending; also how long a partial batch waits once records stop arriving (the task asks Connect to wake it).

  * Type: long
  * Default: 50
  * Importance: medium

``flush.timeout.ms``
  Max ms to wait when flushing in-flight requests.

  * Type: long
  * Default: 30000 (30 seconds)
  * Importance: medium

``flush.synchronously``
  true = preCommit waits for all in-flight writes (default, safest). false = preCommit returns offsets only for writes whose ack arrived; much higher throughput when Solr is slow.

  * Type: boolean
  * Default: true
  * Importance: medium

``max.in.flight.requests``
  Concurrent Solr requests per task. Default 1 = strict Kafka-offset apply order. Raising it only preserves order when kafka.offset.version.field is set (with a DocBasedVersionConstraints processor on that field); otherwise concurrent batches can land out of order and an older update can overwrite a newer one.

  * Type: int
  * Default: 1
  * Valid Values: [1,...]
  * Importance: medium

``ordering.lanes.enabled``
  true = with max.in.flight.requests > 1, route every document to one of max.in.flight.requests lanes by its id. Each lane sends one request at a time in Kafka-offset order and lanes run in parallel, so two writes for the same id never overlap and per-document order holds WITHOUT kafka.offset.version.field. false (default) = the shared pool, where raising max.in.flight.requests needs kafka.offset.version.field. No effect at max.in.flight.requests=1.

  * Type: boolean
  * Default: false
  * Importance: medium

``max.buffered.records``
  Maximum records held in the task's buffers, not yet sent to Solr; reaching it sends them. Records already sent are held back by max.in.flight.requests. With ordering.lanes.enabled every lane fills its own batch, so it must be at least batch.size x max.in.flight.requests (batch.size without lanes).

  * Type: int
  * Default: 20000
  * Importance: medium

``max.retries``
  Maximum retry attempts on retryable failures.

  * Type: int
  * Default: 5
  * Valid Values: [0,...]
  * Importance: medium

``retry.backoff.ms``
  Initial backoff, doubles up to 30s.

  * Type: long
  * Default: 200
  * Importance: low

``retry.timeout.ms``
  How long the task keeps retrying while no write reaches Solr before it fails, so a stalled sink shows as a FAILED task instead of RUNNING. -1 retries forever.

  * Type: long
  * Default: 600000 (10 minutes)
  * Valid Values: [-1,...]
  * Importance: medium

Write
^^^^^

``write.method``
  INDEX replaces the whole document. UPSERT sets only the fields the record carries, removing those it carries as null, and creates a missing document (the Elasticsearch connector's upsert). ATOMIC_UPDATE sets them on an existing document only: a record for a missing one is rejected (HTTP 409). With either, a record that sets no field is skipped.

  * Type: string
  * Default: INDEX
  * Importance: medium

``id.strategy``
  KAFKA_KEY | RECORD_FIELD | TOPIC_PARTITION_OFFSET | UUID.

  * Type: string
  * Default: KAFKA_KEY
  * Importance: medium

``id.field``
  Field path used when id.strategy=RECORD_FIELD.

  * Type: string
  * Default: id
  * Importance: low

``id.coerce.to.string``
  When true (default), numeric ids (Long/Integer) are converted to String before sending to Solr — safe with Solr schemas declaring the id field as 'string'. Set to false when your Solr schema declares id as plong/pint/pdouble; the numeric value is then passed through to SolrJ unchanged, saving one String allocation per record and shrinking the wire payload. Has no effect for TOPIC_PARTITION_OFFSET / UUID id strategies, which always produce strings.

  * Type: boolean
  * Default: true
  * Importance: low

``use.autogenerated.ids``
  When true, force id.strategy=UUID.

  * Type: boolean
  * Default: false
  * Importance: low

``external.version.header``
  Name of a Kafka record header carrying the document version. Empty disables external versioning. When set, the value is written as Solr's _version_ field for optimistic concurrency. Accepts any header type (string/long/bytes).

  * Type: string
  * Default: ""
  * Importance: low

``kafka.offset.version.field``
  When set, every document is stamped with its Kafka offset in this field. Pair it with a Solr DocBasedVersionConstraints update processor (ignoreOldUpdates=true) on the same field so that with max.in.flight.requests > 1, an out-of-order (older-offset) write can never overwrite a newer version of the same id. Empty = disabled.

  * Type: string
  * Default: ""
  * Importance: medium

``source.field``
  When set, the record value is stored verbatim as a JSON string in this field (a stored-only '_source'). Solr flattens nested arrays for indexing, so queries that need the original document (e.g. to return nested custom_fields) read this field back and parse it. Empty = disabled.

  * Type: string
  * Default: ""
  * Importance: medium

``source.exclude.fields``
  Comma-separated field-name PREFIXES to omit from the source.field JSON. Index-only denormalized fields (e.g. custom_field_, funnel_lead_) are excluded so the stored document stays the clean original shape. Empty = keep everything.

  * Type: list
  * Default: ""
  * Importance: low

``collection.naming.strategy``
  TOPIC | STATIC | TOPIC_REGEX (pattern=>replacement).

  * Type: string
  * Default: TOPIC
  * Importance: low

Schema
^^^^^^

``schema.auto.create``
  Auto-create the target collection (SolrCloud).

  * Type: boolean
  * Default: false
  * Importance: low

``auto.create.shards``
  numShards when auto-creating a collection.

  * Type: int
  * Default: 1
  * Importance: low

``auto.create.replication.factor``
  replicationFactor when auto-creating a collection.

  * Type: int
  * Default: 1
  * Importance: low

``auto.create.configset``
  configset name to use when auto-creating.

  * Type: string
  * Default: _default
  * Importance: low

``schema.auto.evolve``
  Auto-add new fields to the managed schema.

  * Type: boolean
  * Default: false
  * Importance: low

``external.resource.usage``
  UNUSED (no check) | REQUIRED (fail if missing) | AUTO (create if missing and schema.auto.create=true, else fail).

  * Type: string
  * Default: AUTO
  * Importance: low

Commit
^^^^^^

``commit.within.ms``
  Solr commitWithin sent with every batch.

  * Type: long
  * Default: 1000 (1 second)
  * Importance: low

Ops
^^^

``dry.run``
  When true, the connector converts records and logs the documents it would write but never actually contacts Solr. Useful for debugging Debezium mappings before pointing at a live cluster.

  * Type: boolean
  * Default: false
  * Importance: low

``mapping.version``
  When set, every Solr document gets a `_mapping_version` field with this value. Lets you query Solr later for docs indexed under an old mapping and reindex them.

  * Type: string
  * Default: ""
  * Importance: low

``partition.fanout.enabled``
  When true, each assigned Kafka partition gets its own BulkProcessor (independent buffers + inflight pool). Multiplies real concurrency by the number of partitions a task owns - useful when `tasks.max` is smaller than your topic partition count. When `tasks.max == partitions`, the default shared processor is already optimal.

  * Type: boolean
  * Default: false
  * Importance: low

Streaming
^^^^^^^^^

``streaming.enabled``
  When true, wrap the Solr client in ConcurrentUpdateHttp2SolrClient so docs are streamed to Solr through a persistent HTTP/2 stream instead of one bulk request per batch. Best for high-RTT (WAN) deployments. Forces sync offset semantics - `flush.synchronously` is treated as true regardless of its setting. Standalone Solr only; SolrCloud should continue to use the default CloudHttp2SolrClient.

  * Type: boolean
  * Default: false
  * Importance: low

``streaming.queue.size``
  Internal queue size of the streaming client. Applies only when `streaming.enabled=true`.

  * Type: int
  * Default: 10000
  * Importance: low

``streaming.threads``
  Worker threads in the streaming client. Applies only when `streaming.enabled=true`.

  * Type: int
  * Default: 4
  * Importance: low

