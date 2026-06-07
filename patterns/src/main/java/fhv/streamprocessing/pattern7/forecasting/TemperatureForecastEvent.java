package fhv.streamprocessing.pattern7.forecasting;

import java.time.Instant;

public record TemperatureForecastEvent(
    String stationId,
    Instant windowStart,
    Instant windowEnd,
    double slopePerSecond,
    double currentAverage,
    double forecastNext24h,
    long observationCount
) {}
