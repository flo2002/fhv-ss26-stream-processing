package fhv.streamprocessing.pattern7.forecasting;

import java.time.Instant;

/**
 * Immutable output event produced by the temperature forecast pattern.
 */
public record TemperatureForecastEvent(
    String stationId,
    Instant windowStart,
    Instant windowEnd,
    double slopePerSecond,
    double currentAverage,
    double forecastNext24h,
    long observationCount
) {}
