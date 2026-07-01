package fhv.streamprocessing.pattern11.weatherregime;

import java.time.LocalDate;

/**
 * Immutable output event produced by the weather regime pattern.
 */
public record WeatherRegimeEvent(
    String stationId,
    LocalDate date,
    int clusterId,
    String clusterLabel,
    double distanceToCenter,
    double avgTemperatureCelsius,
    double minTemperatureCelsius,
    double maxTemperatureCelsius,
    double temperatureRangeCelsius,
    double avgWindSpeedMetersPerSecond,
    double maxWindSpeedMetersPerSecond,
    double skyClarityScore,
    long precipitationObservationCount,
    long rainDurationHours,
    long observationCount
) {
}
