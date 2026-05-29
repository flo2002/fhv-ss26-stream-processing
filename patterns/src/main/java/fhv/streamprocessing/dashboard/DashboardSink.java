package fhv.streamprocessing.dashboard;

import fhv.streamprocessing.model.TemperatureAggregate;
import java.time.LocalDate;
import java.time.OffsetDateTime;

public interface DashboardSink extends AutoCloseable {
    void incrementRawRequests();

    void incrementParsedRequests();

    void recordDailyAverage(String stationDayKey, TemperatureAggregate aggregate);

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

    record DailyAverageRow(
        String stationId,
        LocalDate day,
        Double averageTemperatureCelsius,
        long sampleCount,
        OffsetDateTime updatedAt
    ) {
    }
}
