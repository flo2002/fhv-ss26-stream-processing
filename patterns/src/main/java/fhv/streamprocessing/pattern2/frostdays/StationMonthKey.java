package fhv.streamprocessing.pattern2.frostdays;

import java.time.YearMonth;

public record StationMonthKey(String stationId, YearMonth month) {
    public static StationMonthKey parse(String key) {
        String[] parts = key.split("\\|", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Expected stationMonthKey format stationId|yyyy-MM, got " + key);
        }
        return new StationMonthKey(parts[0], YearMonth.parse(parts[1]));
    }

    public String asKey() {
        return stationId + "|" + month;
    }
}
