package com.una.kafka.connect.solr;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class ParseAsciiLongTest {

    @Test
    void parsesPositiveDecimal() {
        assertThat(SolrWriter.parseAsciiLong("12345")).isEqualTo(12345L);
        assertThat(SolrWriter.parseAsciiLong("12345".getBytes(StandardCharsets.UTF_8))).isEqualTo(12345L);
    }

    @Test
    void parsesNegativeDecimal() {
        assertThat(SolrWriter.parseAsciiLong("-42")).isEqualTo(-42L);
        assertThat(SolrWriter.parseAsciiLong("-42".getBytes(StandardCharsets.UTF_8))).isEqualTo(-42L);
    }

    @Test
    void parsesExplicitPlusSign() {
        assertThat(SolrWriter.parseAsciiLong("+9")).isEqualTo(9L);
        assertThat(SolrWriter.parseAsciiLong("+9".getBytes(StandardCharsets.UTF_8))).isEqualTo(9L);
    }

    @Test
    void parsesZero() {
        assertThat(SolrWriter.parseAsciiLong("0")).isEqualTo(0L);
        assertThat(SolrWriter.parseAsciiLong("0".getBytes(StandardCharsets.UTF_8))).isEqualTo(0L);
    }

    @Test
    void parsesLongMaxAndMin() {
        assertThat(SolrWriter.parseAsciiLong(String.valueOf(Long.MAX_VALUE))).isEqualTo(Long.MAX_VALUE);
        assertThat(SolrWriter.parseAsciiLong(String.valueOf(Long.MIN_VALUE + 1))).isEqualTo(Long.MIN_VALUE + 1);
    }

    @Test
    void stripsLeadingAndTrailingWhitespace() {
        assertThat(SolrWriter.parseAsciiLong("  123  ")).isEqualTo(123L);
        assertThat(SolrWriter.parseAsciiLong(" -7\n")).isEqualTo(-7L);
        assertThat(SolrWriter.parseAsciiLong("\t99\t".getBytes(StandardCharsets.UTF_8))).isEqualTo(99L);
    }

    @Test
    void emptyOrWhitespaceOnlyReturnsNull() {
        assertThat(SolrWriter.parseAsciiLong("")).isNull();
        assertThat(SolrWriter.parseAsciiLong("   ")).isNull();
        assertThat(SolrWriter.parseAsciiLong(new byte[0])).isNull();
        assertThat(SolrWriter.parseAsciiLong("\n\t ".getBytes(StandardCharsets.UTF_8))).isNull();
    }

    @Test
    void signOnlyReturnsNull() {
        assertThat(SolrWriter.parseAsciiLong("-")).isNull();
        assertThat(SolrWriter.parseAsciiLong("+")).isNull();
    }

    @Test
    void nonDigitCharactersReturnNull() {
        assertThat(SolrWriter.parseAsciiLong("12a3")).isNull();
        assertThat(SolrWriter.parseAsciiLong("abc")).isNull();
        assertThat(SolrWriter.parseAsciiLong("1.5")).isNull();
        assertThat(SolrWriter.parseAsciiLong("1 2")).isNull();
    }
}
