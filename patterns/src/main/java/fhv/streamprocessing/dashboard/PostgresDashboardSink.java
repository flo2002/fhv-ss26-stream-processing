package fhv.streamprocessing.dashboard;

import fhv.streamprocessing.pattern1.temperature.DailyAverageTemperatureDashboardStore;
import fhv.streamprocessing.pattern1.temperature.TemperatureAggregate;
import fhv.streamprocessing.pattern10.blizzard.BlizzardDashboardStore;
import fhv.streamprocessing.pattern10.blizzard.BlizzardEvent;
import fhv.streamprocessing.pattern2.frostdays.MonthlyFrostDaysDashboardStore;
import fhv.streamprocessing.pattern3.rapidchange.RapidTemperatureChangeDashboardStore;
import fhv.streamprocessing.pattern3.rapidchange.RapidTemperatureChangeEvent;
import fhv.streamprocessing.pattern4.tourism.TourismWeatherQualityDashboardStore;
import fhv.streamprocessing.pattern4.tourism.TourismWeatherQualityEvent;
import fhv.streamprocessing.pattern5.rainduration.RainDurationAggregate;
import fhv.streamprocessing.pattern5.rainduration.YearlyRainDurationDashboardStore;
import fhv.streamprocessing.pattern6.temperatureranking.AnnualPeakTemperatureRankingDashboardStore;
import fhv.streamprocessing.pattern6.temperatureranking.TemperatureRankingAggregate;
import fhv.streamprocessing.pattern7.forecasting.TemperatureForecastDashboardStore;
import fhv.streamprocessing.pattern7.forecasting.TemperatureForecastEvent;
import fhv.streamprocessing.pattern8.maritime.RouteRecommendationDashboardStore;
import fhv.streamprocessing.pattern8.maritime.RouteRecommendationEvent;
import fhv.streamprocessing.pattern9.wetdry.WetPeriodDashboardStore;
import fhv.streamprocessing.pattern9.wetdry.WetPeriodEvent;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PostgresDashboardSink implements DashboardSink {
    private final Connection connection;
    private final PreparedStatement incrementCounter;
    private final DailyAverageTemperatureDashboardStore dailyAverageStore;
    private final YearlyRainDurationDashboardStore rainDurationStore;
    private final MonthlyFrostDaysDashboardStore frostDaysStore;
    private final AnnualPeakTemperatureRankingDashboardStore temperatureRankingStore;
    private final BlizzardDashboardStore blizzardStore;
    private final RapidTemperatureChangeDashboardStore rapidChangeStore;
    private final TourismWeatherQualityDashboardStore tourismQualityStore;
    private final TemperatureForecastDashboardStore forecastStore;
    private final RouteRecommendationDashboardStore routeRecommendationStore;
    private final WetPeriodDashboardStore wetPeriodStore;

    public PostgresDashboardSink(String jdbcUrl, String user, String password, String stationHistoryUrl) {
        this(jdbcUrl, user, password, stationHistoryUrl, true);
    }

    public PostgresDashboardSink(
        String jdbcUrl,
        String user,
        String password,
        String stationHistoryUrl,
        boolean loadStationMetadata
    ) {
        try {
            connection = connectWithRetry(jdbcUrl, user, password);
            connection.setAutoCommit(true);
            createCommonTables();
            if (loadStationMetadata) {
                StationMetadataLoader.loadInto(connection, stationHistoryUrl);
            }

            dailyAverageStore = new DailyAverageTemperatureDashboardStore(connection);
            rainDurationStore = new YearlyRainDurationDashboardStore(connection);
            frostDaysStore = new MonthlyFrostDaysDashboardStore(connection);
            temperatureRankingStore = new AnnualPeakTemperatureRankingDashboardStore(connection);
            blizzardStore = new BlizzardDashboardStore(connection);
            rapidChangeStore = new RapidTemperatureChangeDashboardStore(connection);
            tourismQualityStore = new TourismWeatherQualityDashboardStore(connection);
            forecastStore = new TemperatureForecastDashboardStore(connection);
            routeRecommendationStore = new RouteRecommendationDashboardStore(connection);
            wetPeriodStore = new WetPeriodDashboardStore(connection);

            temperatureRankingStore.clearExistingRows();
            blizzardStore.clearExistingRows();
            rapidChangeStore.clearExistingRows();
            tourismQualityStore.clearExistingRows();
            forecastStore.clearExistingRows();
            routeRecommendationStore.clearExistingRows();
            wetPeriodStore.clearExistingRows();

            incrementCounter = connection.prepareStatement("""
                INSERT INTO noaa_stream_counts (kind, total, updated_at)
                VALUES (?, 1, now())
                ON CONFLICT (kind)
                DO UPDATE SET total = noaa_stream_counts.total + 1, updated_at = now()
                """);
        } catch (SQLException exception) {
            throw new IllegalStateException("Could not initialize Postgres dashboard sink", exception);
        }
    }

    private static Connection connectWithRetry(String jdbcUrl, String user, String password) throws SQLException {
        SQLException lastException = null;
        for (int attempt = 1; attempt <= 30; attempt++) {
            try {
                return DriverManager.getConnection(jdbcUrl, user, password);
            } catch (SQLException exception) {
                lastException = exception;
                try {
                    Thread.sleep(2_000);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw exception;
                }
            }
        }
        throw lastException;
    }

    @Override
    public synchronized void incrementRawRequests() {
        incrementCounter("raw");
    }

    @Override
    public synchronized void incrementParsedRequests() {
        incrementCounter("parsed");
    }

    @Override
    public synchronized void incrementMarineAisRecords() {
        incrementCounter("marine_ais_records");
    }

    @Override
    public synchronized void incrementMarineBuoyRecords() {
        incrementCounter("marine_buoy_records");
    }

    @Override
    public synchronized void recordDailyAverage(String stationDayKey, TemperatureAggregate aggregate) {
        try {
            dailyAverageStore.record(stationDayKey, aggregate);
        } catch (SQLException exception) {
            throw new IllegalStateException("Could not persist daily station average for " + stationDayKey, exception);
        }
    }

    @Override
    public synchronized void recordYearlyRainDuration(String stationYearKey, RainDurationAggregate aggregate) {
        try {
            rainDurationStore.record(stationYearKey, aggregate);
        } catch (SQLException exception) {
            throw new IllegalStateException("Could not persist yearly rain duration for " + stationYearKey, exception);
        }
    }

    @Override
    public synchronized void recordMonthlyFrostDays(String stationMonthKey, Long frostDayCount) {
        try {
            frostDaysStore.record(stationMonthKey, frostDayCount);
        } catch (SQLException exception) {
            throw new IllegalStateException("Could not persist monthly frost days for " + stationMonthKey, exception);
        }
    }

    @Override
    public synchronized void recordTemperatureWindowRanking(String rankingWindowKey, TemperatureRankingAggregate aggregate) {
        try {
            temperatureRankingStore.record(rankingWindowKey, aggregate);
        } catch (SQLException exception) {
            throw new IllegalStateException("Could not persist temperature ranking window for " + rankingWindowKey, exception);
        }
    }

    @Override
    public synchronized void recordBlizzardEvent(String stationWindowKey, BlizzardEvent event) {
        try {
            blizzardStore.record(stationWindowKey, event);
        } catch (SQLException exception) {
            throw new IllegalStateException("Could not persist blizzard event for " + stationWindowKey, exception);
        }
    }

    @Override
    public synchronized void recordRapidTemperatureChange(String stationWindowKey, RapidTemperatureChangeEvent event) {
        try {
            rapidChangeStore.record(stationWindowKey, event);
        } catch (SQLException exception) {
            throw new IllegalStateException("Could not persist rapid temperature change for " + stationWindowKey, exception);
        }
    }

    @Override
    public synchronized void recordTourismWeatherQuality(String regionWindowKey, TourismWeatherQualityEvent event) {
        try {
            tourismQualityStore.record(regionWindowKey, event);
        } catch (SQLException exception) {
            throw new IllegalStateException("Could not persist tourism weather quality for " + regionWindowKey, exception);
        }
    }

    @Override
    public synchronized void recordTemperatureForecast(String stationForecastKey, TemperatureForecastEvent event) {
        try {
            forecastStore.record(stationForecastKey, event);
        } catch (SQLException exception) {
            throw new IllegalStateException("Could not persist temperature forecast for " + stationForecastKey, exception);
        }
    }

    @Override
    public synchronized void recordRouteRecommendation(String recommendationKey, RouteRecommendationEvent event) {
        try {
            routeRecommendationStore.record(recommendationKey, event);
            incrementCounter("marine_route_recommendations");
        } catch (SQLException exception) {
            throw new IllegalStateException("Could not persist route recommendation for " + recommendationKey, exception);
        }
    }

    @Override
    public synchronized void recordWetPeriod(String wetPeriodKey, WetPeriodEvent event) {
        try {
            wetPeriodStore.record(wetPeriodKey, event);
            incrementCounter("wet_period_events");
        } catch (SQLException exception) {
            throw new IllegalStateException("Could not persist wet period for " + wetPeriodKey, exception);
        }
    }

    @Override
    public synchronized void close() {
        closeQuietly(wetPeriodStore);
        closeQuietly(routeRecommendationStore);
        closeQuietly(forecastStore);
        closeQuietly(tourismQualityStore);
        closeQuietly(rapidChangeStore);
        closeQuietly(blizzardStore);
        closeQuietly(temperatureRankingStore);
        closeQuietly(frostDaysStore);
        closeQuietly(rainDurationStore);
        closeQuietly(dailyAverageStore);
        closeQuietly(incrementCounter);
        closeQuietly(connection);
    }

    private void createCommonTables() throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS noaa_stream_counts (
                    kind text PRIMARY KEY,
                    total bigint NOT NULL DEFAULT 0,
                    updated_at timestamptz NOT NULL DEFAULT now()
                )
                """);
            statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS noaa_station_metadata (
                    station_id text PRIMARY KEY,
                    station_location text,
                    station_name text,
                    country_code text,
                    state_code text,
                    icao_code text,
                    latitude double precision,
                    longitude double precision,
                    elevation_meters double precision,
                    period_begin text,
                    period_end text,
                    updated_at timestamptz NOT NULL DEFAULT now()
                )
                """);
        }
    }

    private void incrementCounter(String kind) {
        try {
            incrementCounter.setString(1, kind);
            incrementCounter.executeUpdate();
        } catch (SQLException exception) {
            throw new IllegalStateException("Could not increment " + kind + " request counter", exception);
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
        }
    }
}
