package fhv.streamprocessing.pattern6.temperatureranking;

import java.time.Instant;

/**
 * Composite business key for ranking window state and output records.
 */
public record RankingWindowKey(long windowStartEpochMs, long windowEndEpochMs) {
    public static RankingWindowKey parse(String key) {
        String[] parts = key.split("\\|", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Expected rankingWindowKey format startEpochMs|endEpochMs, got " + key);
        }
        return new RankingWindowKey(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
    }

    public Instant windowStart() {
        return Instant.ofEpochMilli(windowStartEpochMs);
    }

    public Instant windowEnd() {
        return Instant.ofEpochMilli(windowEndEpochMs);
    }

    public String asKey() {
        return windowStartEpochMs + "|" + windowEndEpochMs;
    }
}
