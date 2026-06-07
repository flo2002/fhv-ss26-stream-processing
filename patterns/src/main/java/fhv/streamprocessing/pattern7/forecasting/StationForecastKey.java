package fhv.streamprocessing.pattern7.forecasting;

import java.time.Instant;

public record StationForecastKey(
    String stationId,
    Instant windowStart,
    Instant windowEnd
) {
    public String asKey() {
        return String.format("%s_%s_%s", stationId, windowStart, windowEnd);
    }

    public static StationForecastKey parse(String key) {
        String[] parts = key.split("_");
        return new StationForecastKey(
            parts[0],
            Instant.parse(parts[1]),
            Instant.parse(parts[2])
        );
    }
}
