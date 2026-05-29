package fhv.streamprocessing.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
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
    @JsonIgnore
    public boolean isUsableForTemperatureAverages() {
        return stationId != null
            && observationDate != null
            && temperatureCelsius != null
            && temperatureQualityCode != null
            && !temperatureQualityCode.equals("9");
    }

    @JsonIgnore
    public String stationDayKey() {
        return stationId + "|" + observationDate;
    }
}
