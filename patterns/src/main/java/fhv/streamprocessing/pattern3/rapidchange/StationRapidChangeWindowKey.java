package fhv.streamprocessing.pattern3.rapidchange;

import java.time.Instant;

/**
 * Composite business key for station rapid change window state and output records.
 */
public record StationRapidChangeWindowKey(
    String stationId,
    long windowStartEpochMs,
    long windowEndEpochMs
) {
    public static StationRapidChangeWindowKey parse(String value) {
        String[] parts = value.split("\\|", 3);
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid station rapid change window key: " + value);
        }
        return new StationRapidChangeWindowKey(parts[0], Long.parseLong(parts[1]), Long.parseLong(parts[2]));
    }

    public String asKey() {
        return stationId + "|" + windowStartEpochMs + "|" + windowEndEpochMs;
    }

    public Instant windowStart() {
        return Instant.ofEpochMilli(windowStartEpochMs);
    }

    public Instant windowEnd() {
        return Instant.ofEpochMilli(windowEndEpochMs);
    }
}
