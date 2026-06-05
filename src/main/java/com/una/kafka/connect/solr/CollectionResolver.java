package com.una.kafka.connect.solr;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Maps a Kafka topic to a Solr collection / core name based on
 * collection.naming.strategy: STATIC, TOPIC, or TOPIC_REGEX
 * (pattern=>replacement, e.g. logs-.*=>logs).
 */
public class CollectionResolver {

    private final SolrSinkConfig config;
    private final Pattern regex;
    private final String replacement;

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
