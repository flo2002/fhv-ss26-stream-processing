package fhv.streamprocessing.pattern4.tourism;

import java.time.Instant;

/**
 * Composite business key for region window state and output records.
 */
public record RegionWindowKey(
    String regionId,
    Instant windowStart,
    Instant windowEnd
) {
    public static RegionWindowKey parse(String value) {
        String[] parts = value.split("\\|", 3);
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid tourism region window key: " + value);
        }
        return new RegionWindowKey(parts[0], Instant.ofEpochMilli(Long.parseLong(parts[1])), Instant.ofEpochMilli(Long.parseLong(parts[2])));
    }

    public static RegionWindowKey of(TourismWeatherQualityEvent event) {
        return new RegionWindowKey(event.regionId(), event.windowStart(), event.windowEnd());
    }

    public String asKey() {
        return regionId + "|" + windowStart.toEpochMilli() + "|" + windowEnd.toEpochMilli();
    }
}
