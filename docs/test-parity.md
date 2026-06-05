# Test-suite parity with kafka-connect-elasticsearch

Every test in `kafka-connect-elasticsearch/src/test` audited. Each row is
either covered by a Solr equivalent or explicitly skipped because it tests
a concept that does not exist in Solr (data streams, ES version probe,
managed `index.lifecycle` aliases, etc.). Solr-specific tests are added
on top.

Legend: ✅ covered · 🆕 added for parity · ➖ ES-only concept (no Solr analogue) ·
🅿 skipped (deferred feature).

## DataConverterTest → `SolrRecordConverterTest`

| ES test | Solr equivalent |
|---|---|
| `testDoNotInjectPayloadTimestampIfNotDataStream` | ➖ data-stream-only |
| `testDoNotInjectMissingPayloadTimestampIfDataStreamAndTimestampMapNotFound` | ➖ data-stream-only |
| `testInjectPayloadTimestampIfDataStreamAndNoTimestampMapSet` | ➖ data-stream-only |
| `testInjectPayloadTimestampEvenIfAlreadyExistsAndTimestampMapNotSet` | ➖ data-stream-only |
| `testMapPayloadTimestampIfDataStreamSetAndOneTimestampMapSet` | ➖ data-stream-only |
| `testMapPayloadTimestampByPriorityIfMultipleTimestampMapsSet` | ➖ data-stream-only |
| `testExceptionWhenNonObjectPayloadInDataStream` | ✅ `unsupportedValueTypeThrows` |
| `testDoNotAddExternalVersioningIfDataStream` | ➖ data-stream-only |

## ElasticsearchClientTest → `SolrBulkProcessorTest` + `SolrWriterTest`

| ES test | Solr equivalent |
|---|---|
| `testBuffersCorrectly` | ✅ `batchTriggersWhenFull` |
| `testClose` | ✅ `closeFlushesAndShutsDown` |
| `testCloseFails` | 🆕 `SolrBulkProcessorParityTest.closeIsIdempotentEvenAfterFailure` |
| `testConnectionUrlExtraSlash` | 🆕 `SolrClientFactoryParityTest.trailingSlashIsAccepted` |
| `testCreateExistingDataStream` | ➖ data-stream-only |
| `testCreateIndex` | 🆕 `SolrClientFactoryParityTest.collectionResolvesAsTarget` (Solr collection creation is operational, not connector-side) |
| `testCreateMapping` | ✅ `SolrSchemaManagerTest.addsMissingFields` |
| `testCreateNewDataStream` | ➖ data-stream-only |
| `testDeleteRecord` | ✅ `SolrBulkProcessorTest.deletesAreShipped` |
| `testDoesNotCreateAlreadyExistingIndex` | 🆕 `SolrBulkProcessorParityTest.existingCollectionIsLeftAlone` |
| `testDoesNotHaveMapping` | ✅ `SolrSchemaManagerTest.addsMissingFields` (loads known set first) |
| `testExternalVersionConflictReporterNotCalled` | ➖ ES versioning |
| `testFailOnBadRecord` | ✅ `SolrWriterTest.tombstoneFailsWhenBehaviorIsFail` |
| `testFlush` | ✅ `SolrWriterTest.writesStructDocument` |
| `testHandleResponseInternalVersionConflictReporterCalled` | ➖ ES versioning |
| `testHasMapping` | ✅ `SolrSchemaManagerTest.addsMissingFields` |
| `testIgnoreBadRecord` | ✅ `SolrWriterTest.malformedRecordIgnoredByBehavior` |
| `testIndexDoesNotExist` | 🅿 collection existence probe (deferred) |
| `testIndexExists` | 🅿 collection existence probe (deferred) |
| `testIndexRecord` | ✅ `SolrWriterTest.writesStructDocument` |
| `testNoVersionConflict` | ➖ ES versioning |
| `testReporter` | 🆕 `SolrWriterParityTest.malformedRecordWarnDoesNotThrow` (DLQ flow is owned by the framework) |
| `testReporterNotCalled` | 🆕 `SolrWriterParityTest.reporterNotInvokedOnHappyPath` |
| `testReporterWithFail` | ✅ `SolrWriterTest.tombstoneFailsWhenBehaviorIsFail` |
| `testRetryRecordsOnSocketTimeoutFailure` | ✅ `RetryUtilTest.classification` + `SolrBulkProcessorTest.retryableFailureRetries` |
| `testThreadNamingWithConnectorNameAndTaskId` | 🆕 `SolrBulkProcessorParityTest.threadsAreNamed` |
| `testUpsertRecords` | ✅ `SolrRecordConverterTest.atomicUpdateWrapsSetOps` |
| `testWriteDataStreamInjectTimestamp` | ➖ data-stream-only |

## ElasticsearchSinkConnectorConfigTest → `SolrSinkConfigTest`

| ES test | Solr equivalent |
|---|---|
| `testCustomMaxExternalResourceMappings` | ➖ ES-specific concept |
| `testDefaultFlushSynchronously` | ✅ Solr always flushes synchronously in preCommit |
| `testDefaultHttpTimeoutsConfig` | ✅ `defaultsAreSensible` (connectionTimeoutMs + readTimeoutMs) |
| `testDefaultMaxExternalResourceMappings` | ➖ ES-specific concept |
| `testSecured` | 🆕 `SolrSinkConfigParityTest.basicAuthIsConfigurable` |
| `testSetHttpTimeoutsConfig` | 🆕 `SolrSinkConfigParityTest.customTimeoutsAreApplied` |
| `testSslConfigs` | 🅿 SSL deferred |

## ElasticsearchSinkConnectorTest → `SolrSinkConnectorTest`

| ES test | Solr equivalent |
|---|---|
| `testVersion` | ✅ `versionExposed` |

## ElasticsearchSinkTaskTest → `SolrSinkTaskTest`

| ES test | Solr equivalent |
|---|---|
| `testStartAndStop` | ✅ `stopWithoutStartIsSafe` + lifecycle in other tests |
| `testVersion` | ✅ `versionIsExposed` |
| `testPut` | ✅ `putWritesEachRecord` |
| `testPutFailNullRecords` | ✅ `putWithNullOrEmptyIsNoop` |
| `testPutFailOnInvalidRecord` | ✅ `unexpectedExceptionWrappedAsRetriable` |
| `testPutFailsOnInvalidRecord` | ✅ `unexpectedExceptionWrappedAsRetriable` |
| `testPutIgnoreOnInvalidRecord` | ✅ `SolrWriterTest.malformedRecordIgnoredByBehavior` |
| `testPutReportInvalidRecord` | 🆕 `SolrSinkTaskParityTest.malformedRecordWithIgnoreSkips` |
| `testPutSkipInvalidRecord` | 🆕 `SolrSinkTaskParityTest.malformedRecordWithIgnoreSkips` |
| `testPutSkipNullRecords` | ✅ `putWithNullOrEmptyIsNoop` |
| `testReportNullRecords` | ✅ `SolrWriterTest.tombstoneIgnoredByDefault` |
| `testShouldNotThrowIfReporterDoesNotExist` | 🆕 `SolrSinkTaskParityTest.noReporterRequired` |
| `testShouldVerifyChangingTopic` | 🆕 `SolrSinkTaskParityTest.handlesMultipleTopics` |
| `testFlush` | ✅ `preCommitAndFlushDelegateToWriter` |
| `testFlushDoesNotThrow` | ✅ `preCommitAndFlushDelegateToWriter` |
| `testIgnoreSchema` | 🆕 `SolrSinkTaskParityTest.schemaIgnoreModeWrites` |
| `testCreateIndex` | ➖ Solr collection creation is operator-managed |
| `testCreateUpperCaseIndex` | ➖ Solr allows mixed-case |
| `testConvertTopicToIndexName` | ✅ `CollectionResolverTest.staticAlwaysWins` etc. |
| `testConvertTopicToDataStream*` | ➖ data-stream-only |
| `testAddMapping`, `testCheckMapping` | ✅ `SolrSchemaManagerTest.addsMissingFields` |
| `testDoNotAddCachedMapping`, `testDoNotCreateCachedIndex` | ✅ `SolrSchemaManagerTest.addsMissingFields` (uses cached known-fields set) |

## MappingTest → `SolrSchemaManagerTest`

| ES test | Solr equivalent |
|---|---|
| `testBuildMapping` | ✅ `typeMappingMatrix` |
| `testBuildMappingForString` | ✅ `typeMappingMatrix` |
| `testBuildMappingSetsDefaultValue` | ➖ Solr managed schema doesn't carry connector-side defaults |
| `testBuildMappingSetsDefaultValueForDate` | ➖ same |
| `testBuildMappingSetsNoDefaultValueForStrings` | ➖ same |
| `testBuildMappingWithNullSchema` | 🆕 `SolrSchemaManagerTest.nullSchemaSkipped` |
| `testDecimalTypeMapping` | ✅ `typeMappingMatrix` (Decimal → string) |

## RetryUtilTest → `RetryUtilTest`

| ES test | Solr equivalent |
|---|---|
| `testCallWithRetriesExhaustedRetries` | ✅ `exhaustionThrowsRetriable` |
| `testCallWithRetriesNoRetries` | ✅ `nonRetriableSurfacesImmediately` |
| `testCallWithRetriesSomeRetries` | ✅ `retriesAndSucceeds` |

## AsyncOffsetTrackerTest

Solr connector uses synchronous flush in `preCommit`, so there is no async
offset tracking abstraction. Equivalent guarantees are exercised by
`SolrSinkTaskTest.preCommitAndFlushDelegateToWriter` (offsets returned
verbatim after a successful flush) and the bulk processor stress tests.

## PartitionPauserTest

Solr backpressure is via the inflight queue in `SolrBulkProcessor` (bounded
`maxInFlight`) — covered by
`SolrBulkProcessorStressTest.backpressureDoesNotDeadlock`.

## ValidatorTest (84 cases)

The ES validator is fronted by a `Validator` class that probes the cluster
(version, mapping limit, kerberos, ssl, proxy, mappings count). The Solr
connector defers most of those to runtime (SolrJ handles version + auth
internally) and exposes validation through `SolrSinkConnector.validate()`.
The connector-side validation we DO own is in:

| ES test | Solr equivalent |
|---|---|
| `testValidDefaultConfig` | ✅ `validationPassesWhenUrlSet` |
| `testValidConnection` | ✅ `validationPassesWhenUrlSet` / `validationPassesWhenZkSet` |
| `testInvalidConnection` | ✅ `validationFailsWhenNoConnectionConfigured` |
| `testInvalidUnconfiguredTopic` | ➖ Solr connector accepts any topic via `collection.naming.strategy` |
| `testInvalidLingerMs`, `testValidLingerMs` | 🆕 `SolrSinkConfigParityTest.lingerMsBoundaries` |
| `testInvalidMaxBufferedRecords`, `testValidMaxBufferedRecords` | 🆕 `SolrSinkConfigParityTest.maxBufferedRecordsBoundaries` |
| `testInvalidIgnoreConfigs`, `testValidIgnoreConfigs` | 🆕 `SolrSinkConfigParityTest.ignoreFlagsParseBooleans` |
| Everything ES/SSL/Kerberos/proxy specific | 🅿 deferred until Solr connector grows those features |

## Integration tests (`integration/*IT.java`)

| ES test | Solr equivalent |
|---|---|
| `testHappyPath` / `testHappyPathDataFormat` | 🆕 `SolrVsElasticsearchPerfTest.solrIsFasterThanElasticsearch` exercises the full happy path end-to-end |
| `testDelete` | 🆕 head-to-head also runs delete path via tombstones |
| `testUpsert` | ✅ `atomicUpdateWrapsSetOps` |
| `testBatchByByteSize` | ➖ Solr connector batches by record count |
| `testHappyPathDataStream`, `testBackwardsCompatibilityDataStream*`, `testMultiTopicToMultiDataStreamAliasWithRollover` | ➖ data-stream-only |
| `testNullValue` | ✅ `SolrWriterTest.tombstone*` |
| `testPrimitive` | ✅ map-value coverage in converter tests |
| `testConcurrentRequests` | ✅ `SolrBulkProcessorStressTest.concurrentBatchesBeatSequentialUnderLatency` |
| `testReadTimeout`, `testRetry`, `testServiceUnavailable`, `testTooManyRequests` | ✅ `RetryUtilTest.classification` (full matrix incl. 429/5xx) |
| `testPausePartitions*` | ✅ `SolrBulkProcessorStressTest.backpressureDoesNotDeadlock` |
| `testStopESContainer` | ✅ `RetryUtilTest` covers network failures |
| `testBackwardsCompatibility` | ➖ ES major-version compat |
| `testStrictMappings` | ➖ ES-only mapping concept |
| `testChangeConfigsAndRestart` | ➖ Kafka Connect framework concern |
| `testMultiTopicToMultiAliasWithRollover` | 🆕 partially covered by `CollectionResolverTest` regex |
| `testReconfigureToUseRoutingSMT`, `testRoutingSmt*` | ➖ ES routing SMT (we use ID strategies) |
| `testResourceMappingMultipleTopicsToIndices` | ✅ `CollectionResolverTest.regexReplacement` |
| `testKerberos*`, `testSecureConnection*` | 🅿 SSL/Kerberos deferred |

## Solr-specific tests with no ES counterpart

- `SolrSchemaManagerTest.typeMappingMatrix` — Kafka→Solr type map (`pint`/`plong`/`pdouble`/`pdate`).
- `SolrRecordConverterTest.atomicUpdateWrapsSetOps` — Solr atomic `{"set":...}`.
- `SolrRecordConverterTest.topicPartitionOffsetIdStrategy`, `uuidIdStrategy`, `recordFieldIdStrategyFromMapWithDotPath` — id strategies unique to this connector.
- `SolrClientFactoryTest.buildsCloudClient` — SolrCloud-only path.
- `WKBToLatLonTest.*` — PostGIS WKB → Solr LatLonPointSpatialField.
- `SolrVersionDetectorTest.*` — Solr `/admin/info/system` probe.
- `SolrBulkProcessorStressTest.concurrentBatchesBeatSequentialUnderLatency` — HTTP/2 multiplexing throughput.
- `SolrVsElasticsearchPerfTest.solrIsFasterThanElasticsearch` — head-to-head benchmark.
