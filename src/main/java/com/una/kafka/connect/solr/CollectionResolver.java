package com.una.kafka.connect.solr;

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

    private final SolrSinkConfig config;
    private final Pattern regex;
    private final String replacement;
    private final ConcurrentMap<String, String> cache = new ConcurrentHashMap<>();

    public CollectionResolver(SolrSinkConfig config) {
        this.config = config;
        if ("TOPIC_REGEX".equalsIgnoreCase(config.collectionNamingStrategy())) {
            String raw = config.defaultCollection();
            int idx = raw.indexOf("=>");
            if (idx < 0) {
                throw new IllegalArgumentException(
                        "collection.naming.strategy=TOPIC_REGEX requires solr.collection "
                                + "in 'pattern=>replacement' form, got: " + raw);
            }
            this.regex = Pattern.compile(raw.substring(0, idx).trim());
            this.replacement = raw.substring(idx + 2).trim();
        } else {
            this.regex = null;
            this.replacement = null;
        }
    }

    public String resolve(String topic) {
        String cached = cache.get(topic);
        if (cached != null) {
            return cached;
        }
        // computeIfAbsent would be cleaner but allocates a lambda capture
        // on every call. The double-get pattern avoids that on the hot path.
        String computed = compute(topic);
        String existing = cache.putIfAbsent(topic, computed);
        return existing != null ? existing : computed;
    }

    private String compute(String topic) {
        switch (config.collectionNamingStrategy().toUpperCase()) {
            case "STATIC":
                return config.defaultCollection().isEmpty() ? topic : config.defaultCollection();
            case "TOPIC_REGEX":
                Matcher m = regex.matcher(topic);
                if (m.matches()) {
                    return m.replaceAll(replacement);
                }
                return topic;
            case "TOPIC":
            default:
                return config.defaultCollection().isEmpty() ? topic : config.defaultCollection();
        }
    }
}
