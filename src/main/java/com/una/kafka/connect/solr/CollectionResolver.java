package com.una.kafka.connect.solr;

import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Maps a Kafka topic to a Solr collection / core name.
 *
 * <p>The result is deterministic per topic, so we memoise it: first call
 * for a topic pays the dispatch cost (regex match / switch), every
 * subsequent call is a single {@link ConcurrentHashMap#get(Object)}.</p>
 *
 * <p>This matters for the {@code TOPIC_REGEX} strategy where the cost of
 * running {@link Matcher#matches()} every record is non-trivial; for the
 * {@code STATIC} / {@code TOPIC} strategies the cache still saves the
 * per-record {@code switch} dispatch.</p>
 *
 * <p>Strategies (configurable via {@code collection.naming.strategy}):</p>
 * <ul>
 *     <li><b>STATIC</b> - always use {@code solr.collection}.</li>
 *     <li><b>TOPIC</b> - use the topic name verbatim (or {@code solr.collection}
 *         when set).</li>
 *     <li><b>TOPIC_REGEX</b> - take {@code solr.collection} as
 *         {@code pattern=>replacement}, e.g. {@code logs-.*=>logs}.</li>
 * </ul>
 */
public final class CollectionResolver {

    private enum Strategy { STATIC, TOPIC, TOPIC_REGEX }

    private final Strategy strategy;
    private final String defaultCollection;
    private final Pattern regex;
    private final String replacement;
    private final ConcurrentMap<String, String> cache = new ConcurrentHashMap<>();
    private volatile String lastTopic;
    private volatile String lastResolved;

    public CollectionResolver(SolrSinkConfig config) {
        String raw = config.collectionNamingStrategy();
        this.strategy = parseStrategy(raw);
        this.defaultCollection = config.defaultCollection();
        if (strategy == Strategy.TOPIC_REGEX) {
            int idx = defaultCollection.indexOf("=>");
            if (idx < 0) {
                throw new IllegalArgumentException(
                        "collection.naming.strategy=TOPIC_REGEX requires solr.collection "
                                + "in 'pattern=>replacement' form, got: " + defaultCollection);
            }
            this.regex = Pattern.compile(defaultCollection.substring(0, idx).trim());
            this.replacement = defaultCollection.substring(idx + 2).trim();
        } else {
            this.regex = null;
            this.replacement = null;
        }
    }

    private static Strategy parseStrategy(String raw) {
        if (raw == null) return Strategy.TOPIC;
        switch (raw.toUpperCase(Locale.ROOT)) {
            case "STATIC":      return Strategy.STATIC;
            case "TOPIC_REGEX": return Strategy.TOPIC_REGEX;
            case "TOPIC":
            default:            return Strategy.TOPIC;
        }
    }

    public String resolve(String topic) {
        String lt = lastTopic;
        if (lt != null && lt.equals(topic)) {
            return lastResolved;
        }
        String cached = cache.get(topic);
        if (cached != null) {
            lastTopic = topic;
            lastResolved = cached;
            return cached;
        }
        String computed = compute(topic);
        String existing = cache.putIfAbsent(topic, computed);
        String result = existing != null ? existing : computed;
        lastTopic = topic;
        lastResolved = result;
        return result;
    }

    private String compute(String topic) {
        switch (strategy) {
            case STATIC:
                return defaultCollection.isEmpty() ? topic : defaultCollection;
            case TOPIC_REGEX:
                Matcher m = regex.matcher(topic);
                return m.matches() ? m.replaceAll(replacement) : topic;
            case TOPIC:
            default:
                return defaultCollection.isEmpty() ? topic : defaultCollection;
        }
    }
}
