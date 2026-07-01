package fhv.streamprocessing.model;

import java.time.LocalDate;
import java.time.OffsetDateTime;

/**
 * Normalized weather observation shared by all stream-processing patterns.
 */
public record NoaaObservation(
    String stationId,
    LocalDate observationDate,
    OffsetDateTime observedAt,
    Double temperatureCelsius,
    String temperatureQualityCode,
    Double windSpeedMetersPerSecond,
    String windSpeedQualityCode,
    Double skyClarityScore,
    String visibilityQualityCode,
    Integer rainDurationHours,
    String sourcePath,
    long recordNumber,
    String rawPayload
) {
}
