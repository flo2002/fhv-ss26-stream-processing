package fhv.streamprocessing.pattern5.rainduration;

import fhv.streamprocessing.model.NoaaObservation;

public record StationYearKey(String stationId, int year) {
    public static StationYearKey fromObservation(NoaaObservation observation) {
        return new StationYearKey(observation.stationId(), observation.observationDate().getYear());
    }

    public static StationYearKey parse(String key) {
        String[] parts = key.split("\\|", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Expected stationYearKey format stationId|yyyy, got " + key);
        }
        return new StationYearKey(parts[0], Integer.parseInt(parts[1]));
    }

    public String asKey() {
        return stationId + "|" + year;
    }
}
