package fhv.streamprocessing.dashboard;

import fhv.streamprocessing.pattern1.temperature.TemperatureAggregate;
import fhv.streamprocessing.pattern5.rainduration.RainDurationAggregate;
import fhv.streamprocessing.pattern6.temperatureranking.TemperatureRankingAggregate;

public interface DashboardSink extends AutoCloseable {
    void incrementRawRequests();

    void incrementParsedRequests();

    void recordDailyAverage(String stationDayKey, TemperatureAggregate aggregate);

    void recordYearlyRainDuration(String stationYearKey, RainDurationAggregate aggregate);

    void recordMonthlyFrostDays(String stationMonthKey, Long frostDayCount);

    void recordTemperatureWindowRanking(String rankingWindowKey, TemperatureRankingAggregate aggregate);

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
            public void recordTemperatureWindowRanking(String rankingWindowKey, TemperatureRankingAggregate aggregate) {
            }

            @Override
            public void close() {
            }
        };
    }
}
