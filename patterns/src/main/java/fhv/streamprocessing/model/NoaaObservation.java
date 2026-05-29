package fhv.streamprocessing.model;

import java.time.LocalDate;
import java.time.OffsetDateTime;

public record NoaaObservation(
    String stationId,
    LocalDate observationDate,
    OffsetDateTime observedAt,
    Double temperatureCelsius,
    String temperatureQualityCode,
    String sourcePath,
    long recordNumber,
    String rawPayload
) {
    public boolean isUsableForTemperatureAverages() {
        return stationId != null
            && observationDate != null
            && temperatureCelsius != null
            && temperatureQualityCode != null
            && !temperatureQualityCode.equals("9");
    }

    public String stationDayKey() {
        return stationId + "|" + observationDate;
    }
}
