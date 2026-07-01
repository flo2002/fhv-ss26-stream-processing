package fhv.streamprocessing.pattern4.tourism;

import java.time.Instant;

/**
 * Immutable output event produced by the tourism weather quality pattern.
 */
public record TourismWeatherQualityEvent(
    String regionId,
    String regionName,
    Instant windowStart,
    Instant windowEnd,
    double avgTemperatureCelsius,
    double avgWindSpeedMetersPerSecond,
    double skyClarityScore,
    double qualityScore,
    String qualityClass,
    long observationCount
) {
}
