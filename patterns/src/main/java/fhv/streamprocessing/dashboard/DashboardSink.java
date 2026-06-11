package fhv.streamprocessing.dashboard;

import fhv.streamprocessing.pattern1.temperature.TemperatureAggregate;
import fhv.streamprocessing.pattern10.blizzard.BlizzardEvent;
import fhv.streamprocessing.pattern3.rapidchange.RapidTemperatureChangeEvent;
import fhv.streamprocessing.pattern4.tourism.TourismWeatherQualityEvent;
import fhv.streamprocessing.pattern5.rainduration.RainDurationAggregate;
import fhv.streamprocessing.pattern6.temperatureranking.TemperatureRankingAggregate;
import fhv.streamprocessing.pattern7.forecasting.TemperatureForecastEvent;
import fhv.streamprocessing.pattern8.maritime.RouteRecommendationEvent;
import fhv.streamprocessing.pattern9.wetdry.WetPeriodEvent;
import fhv.streamprocessing.pattern11.weatherregime.WeatherRegimeEvent;

public interface DashboardSink extends AutoCloseable {
    void incrementRawRequests();

    void incrementParsedRequests();

    void incrementMarineAisRecords();

    void incrementMarineBuoyRecords();

    void recordDailyAverage(String stationDayKey, TemperatureAggregate aggregate);

    void recordYearlyRainDuration(String stationYearKey, RainDurationAggregate aggregate);

    void recordMonthlyFrostDays(String stationMonthKey, Long frostDayCount);

    void recordTemperatureWindowRanking(String rankingWindowKey, TemperatureRankingAggregate aggregate);

    void recordBlizzardEvent(String stationWindowKey, BlizzardEvent event);

    void recordRapidTemperatureChange(String stationWindowKey, RapidTemperatureChangeEvent event);

    void recordTourismWeatherQuality(String regionWindowKey, TourismWeatherQualityEvent event);

    void recordTemperatureForecast(String stationForecastKey, TemperatureForecastEvent event);

    void recordRouteRecommendation(String recommendationKey, RouteRecommendationEvent event);

    void recordWetPeriod(String wetPeriodKey, WetPeriodEvent event);

    void recordWeatherRegime(String stationDayKey, WeatherRegimeEvent event);

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
            public void incrementMarineAisRecords() {
            }

            @Override
            public void incrementMarineBuoyRecords() {
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
            public void recordBlizzardEvent(String stationWindowKey, BlizzardEvent event) {
            }

            @Override
            public void recordRapidTemperatureChange(String stationWindowKey, RapidTemperatureChangeEvent event) {
            }

            @Override
            public void recordTourismWeatherQuality(String regionWindowKey, TourismWeatherQualityEvent event) {
            }

            @Override
            public void recordTemperatureForecast(String stationForecastKey, TemperatureForecastEvent event) {
            }

            @Override
            public void recordRouteRecommendation(String recommendationKey, RouteRecommendationEvent event) {
            }

            @Override
            public void recordWetPeriod(String wetPeriodKey, WetPeriodEvent event) {
            }

            @Override
            public void recordWeatherRegime(String stationDayKey, WeatherRegimeEvent event) {
            }

            @Override
            public void close() {
            }
        };
    }
}
