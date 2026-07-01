package fhv.streamprocessing.pattern10.blizzard;

import java.time.Instant;

/**
 * Composite business key for station blizzard window state and output records.
 */
public record StationBlizzardWindowKey(
    String stationId,
    long windowStartEpochMs,
    long windowEndEpochMs
) {
    public static StationBlizzardWindowKey parse(String value) {
        String[] parts = value.split("\\|", 3);
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid station blizzard window key: " + value);
        }
        return new StationBlizzardWindowKey(parts[0], Long.parseLong(parts[1]), Long.parseLong(parts[2]));
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
