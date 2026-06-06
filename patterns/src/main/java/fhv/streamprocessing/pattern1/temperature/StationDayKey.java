package fhv.streamprocessing.pattern1.temperature;

import fhv.streamprocessing.model.NoaaObservation;
import java.time.LocalDate;

public record StationDayKey(String stationId, LocalDate day) {
    public static StationDayKey fromObservation(NoaaObservation observation) {
        return new StationDayKey(observation.stationId(), observation.observationDate());
    }

    public static StationDayKey parse(String key) {
        String[] parts = key.split("\\|", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Expected stationDayKey format stationId|yyyy-MM-dd, got " + key);
        }
        return new StationDayKey(parts[0], LocalDate.parse(parts[1]));
    }

    public String asKey() {
        return stationId + "|" + day;
    }
}
