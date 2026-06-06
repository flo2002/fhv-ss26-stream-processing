package fhv.streamprocessing.model;

import java.time.LocalDate;
import java.time.OffsetDateTime;

public record NoaaObservation(
    String stationId,
    LocalDate observationDate,
    OffsetDateTime observedAt,
    Double temperatureCelsius,
    String temperatureQualityCode,
    Double windSpeedMetersPerSecond,
    String windSpeedQualityCode,
    Integer rainDurationHours,
    String sourcePath,
    long recordNumber,
    String rawPayload
) {
}
