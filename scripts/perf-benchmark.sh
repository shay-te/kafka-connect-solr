#!/usr/bin/env bash
# Head-to-head Solr-vs-Elasticsearch connector benchmark.
#
# Needs a working Docker daemon: the Testcontainers-based SolrVsElasticsearchPerfTest starts a
# real solr:9.4 and a real elasticsearch:7.17.24 and pushes the same load through both. Read the
# "[HEAD-TO-HEAD]" line in the output; JFR recordings land in target/perf-jfr.
#
# Run it on any Docker-capable box (laptop or CI runner):
#     ./scripts/perf-benchmark.sh
#
# Deliberately a script and NOT a .github/workflows/ file: the push credential for this repo is a
# PAT without the `workflow` scope, so any commit touching .github/workflows/ is rejected outright
# and takes the whole branch push down with it.
set -euo pipefail

cd "$(dirname "$0")/.."

if ! docker info >/dev/null 2>&1; then
  echo "docker is not available — the head-to-head benchmark needs a running Docker daemon." >&2
  exit 1
fi

# -Pperf selects the @Tag("performance") tests; -Dsurefire.useFile=false streams the
# [HEAD-TO-HEAD] printf straight to stdout instead of only into a report file.
exec mvn -B test -Pperf \
  -Dtest=SolrVsElasticsearchPerfTest \
  -Dsurefire.useFile=false
