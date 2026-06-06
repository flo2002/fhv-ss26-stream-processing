package fhv.streamprocessing.dashboard;

import fhv.streamprocessing.model.RainDurationAggregate;
import fhv.streamprocessing.model.TemperatureAggregate;
import java.time.LocalDate;
import java.time.YearMonth;

public interface DashboardSink extends AutoCloseable {
    void incrementRawRequests();

    void incrementParsedRequests();

    void recordDailyAverage(String stationDayKey, TemperatureAggregate aggregate);

    void recordYearlyRainDuration(String stationYearKey, RainDurationAggregate aggregate);

    void recordMonthlyFrostDays(String stationMonthKey, Long frostDayCount);

    @Override
    void close();

    static DashboardSink noop() {
        return new DashboardSink() {
            @Override
            public void incrementRawRequests() {
            }

            @Override
            public void incrementParsedRequests() {
            }

            @Override
            public void recordDailyAverage(String stationDayKey, TemperatureAggregate aggregate) {
            }

            @Override
            public void recordYearlyRainDuration(String stationYearKey, RainDurationAggregate aggregate) {
            }

            @Override
            public void recordMonthlyFrostDays(String stationMonthKey, Long frostDayCount) {
            }

            @Override
            public void close() {
            }
        };
    }

    static StationDay stationDay(String key) {
        String[] parts = key.split("\\|", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Expected stationDayKey format stationId|yyyy-MM-dd, got " + key);
        }
        return new StationDay(parts[0], LocalDate.parse(parts[1]));
    }

    record StationDay(String stationId, LocalDate day) {
    }

    static StationYear stationYear(String key) {
        String[] parts = key.split("\\|", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Expected stationYearKey format stationId|yyyy, got " + key);
        }
        return new StationYear(parts[0], Integer.parseInt(parts[1]));
    }

    record StationYear(String stationId, int year) {
    }

    static StationMonth stationMonth(String key) {
        String[] parts = key.split("\\|", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Expected stationMonthKey format stationId|yyyy-MM, got " + key);
        }
        return new StationMonth(parts[0], YearMonth.parse(parts[1]));
    }

    record StationMonth(String stationId, YearMonth month) {
    }
}
