package fhv.streamprocessing.dashboard;

import fhv.streamprocessing.model.RainDurationAggregate;
import fhv.streamprocessing.model.TemperatureAggregate;
import fhv.streamprocessing.model.TemperatureRankingAggregate;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;

public class PostgresDashboardSink implements DashboardSink {
    private final Connection connection;
    private final PreparedStatement incrementCounter;
    private final PreparedStatement upsertDailyAverage;
    private final PreparedStatement upsertYearlyRainDuration;
    private final PreparedStatement upsertMonthlyFrostDays;
    private final PreparedStatement deleteTemperatureRankings;
    private final PreparedStatement insertDailyTemperatureRanking;

    public PostgresDashboardSink(String jdbcUrl, String user, String password, String stationHistoryUrl) {
        try {
            connection = connectWithRetry(jdbcUrl, user, password);
            connection.setAutoCommit(true);
            createTables();
            StationMetadataLoader.loadInto(connection, stationHistoryUrl);
            incrementCounter = connection.prepareStatement("""
                INSERT INTO noaa_stream_counts (kind, total, updated_at)
                VALUES (?, 1, now())
                ON CONFLICT (kind)
                DO UPDATE SET total = noaa_stream_counts.total + 1, updated_at = now()
                """);
            upsertDailyAverage = connection.prepareStatement("""
                INSERT INTO noaa_daily_station_average (
                    station_id,
                    observation_day,
                    avg_temperature_celsius,
                    sample_count,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (station_id, observation_day)
                DO UPDATE SET
                    avg_temperature_celsius = excluded.avg_temperature_celsius,
                    sample_count = excluded.sample_count,
                    updated_at = excluded.updated_at
                """);
            upsertYearlyRainDuration = connection.prepareStatement("""
                INSERT INTO noaa_yearly_station_rain_duration (
                    station_id,
                    observation_year,
                    avg_duration_hours,
                    sample_count,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (station_id, observation_year)
                DO UPDATE SET
                    avg_duration_hours = excluded.avg_duration_hours,
                    sample_count = excluded.sample_count,
                    updated_at = excluded.updated_at
                """);
            upsertMonthlyFrostDays = connection.prepareStatement("""
                INSERT INTO noaa_monthly_station_frost_days (
                    station_id,
                    observation_month,
                    frost_day_count,
                    updated_at
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT (station_id, observation_month)
                DO UPDATE SET
                    frost_day_count = excluded.frost_day_count,
                    updated_at = excluded.updated_at
                """);
            deleteTemperatureRankings = connection.prepareStatement("""
                DELETE FROM noaa_temperature_window_ranking
                """);
            insertDailyTemperatureRanking = connection.prepareStatement("""
                INSERT INTO noaa_temperature_window_ranking (
                    window_start,
                    window_end,
                    ranking_type,
                    rank_position,
                    station_id,
                    peak_temperature_celsius,
                    avg_temperature_celsius,
                    sample_count,
                    min_temperature_celsius,
                    max_temperature_celsius,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (window_start, window_end, ranking_type, rank_position)
                DO UPDATE SET
                    station_id = excluded.station_id,
                    peak_temperature_celsius = excluded.peak_temperature_celsius,
                    avg_temperature_celsius = excluded.avg_temperature_celsius,
                    sample_count = excluded.sample_count,
                    min_temperature_celsius = excluded.min_temperature_celsius,
                    max_temperature_celsius = excluded.max_temperature_celsius,
                    updated_at = excluded.updated_at
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
    public synchronized void recordDailyAverage(String stationDayKey, TemperatureAggregate aggregate) {
        DashboardSink.StationDay stationDay = DashboardSink.stationDay(stationDayKey);
        try {
            upsertDailyAverage.setString(1, stationDay.stationId());
            upsertDailyAverage.setDate(2, Date.valueOf(stationDay.day()));
            upsertDailyAverage.setDouble(3, aggregate.averageTemperatureCelsius());
            upsertDailyAverage.setLong(4, aggregate.getCount());
            upsertDailyAverage.setTimestamp(5, Timestamp.from(OffsetDateTime.now().toInstant()));
            upsertDailyAverage.executeUpdate();
        } catch (SQLException exception) {
            throw new IllegalStateException("Could not upsert daily station average for " + stationDayKey, exception);
        }
    }

    @Override
    public synchronized void recordYearlyRainDuration(String stationYearKey, RainDurationAggregate aggregate) {
        DashboardSink.StationYear stationYear = DashboardSink.stationYear(stationYearKey);
        try {
            upsertYearlyRainDuration.setString(1, stationYear.stationId());
            upsertYearlyRainDuration.setInt(2, stationYear.year());
            upsertYearlyRainDuration.setDouble(3, aggregate.averageDurationHours());
            upsertYearlyRainDuration.setLong(4, aggregate.getCount());
            upsertYearlyRainDuration.setTimestamp(5, Timestamp.from(OffsetDateTime.now().toInstant()));
            upsertYearlyRainDuration.executeUpdate();
        } catch (SQLException exception) {
            throw new IllegalStateException("Could not upsert yearly rain duration for " + stationYearKey, exception);
        }
    }

    @Override
    public synchronized void recordMonthlyFrostDays(String stationMonthKey, Long frostDayCount) {
        DashboardSink.StationMonth stationMonth = DashboardSink.stationMonth(stationMonthKey);
        try {
            upsertMonthlyFrostDays.setString(1, stationMonth.stationId());
            upsertMonthlyFrostDays.setDate(2, Date.valueOf(stationMonth.month().atDay(1)));
            upsertMonthlyFrostDays.setLong(3, frostDayCount);
            upsertMonthlyFrostDays.setTimestamp(4, Timestamp.from(OffsetDateTime.now().toInstant()));
            upsertMonthlyFrostDays.executeUpdate();
        } catch (SQLException exception) {
            throw new IllegalStateException("Could not upsert monthly frost days for " + stationMonthKey, exception);
        }
    }

    @Override
    public synchronized void recordTemperatureWindowRanking(String rankingWindowKey, TemperatureRankingAggregate aggregate) {
        try {
            DashboardSink.RankingWindow rankingWindow = DashboardSink.rankingWindow(rankingWindowKey);
            Timestamp windowStart = Timestamp.from(rankingWindow.windowStart());
            Timestamp windowEnd = Timestamp.from(rankingWindow.windowEnd());
            deleteTemperatureRankings.executeUpdate();

            Timestamp updatedAt = Timestamp.from(OffsetDateTime.now().toInstant());
            insertRankingRows(windowStart, windowEnd, "hot", aggregate.hottestStations(10), updatedAt);
            insertRankingRows(windowStart, windowEnd, "cold", aggregate.coldestStations(10), updatedAt);
        } catch (SQLException exception) {
            throw new IllegalStateException("Could not upsert temperature ranking window for " + rankingWindowKey, exception);
        }
    }

    @Override
    public synchronized void close() {
        closeQuietly(insertDailyTemperatureRanking);
        closeQuietly(deleteTemperatureRankings);
        closeQuietly(upsertMonthlyFrostDays);
        closeQuietly(upsertYearlyRainDuration);
        closeQuietly(upsertDailyAverage);
        closeQuietly(incrementCounter);
        closeQuietly(connection);
    }

    private void createTables() throws SQLException {
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
            statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS noaa_daily_station_average (
                    station_id text NOT NULL,
                    observation_day date NOT NULL,
                    avg_temperature_celsius double precision NOT NULL,
                    sample_count bigint NOT NULL,
                    updated_at timestamptz NOT NULL DEFAULT now(),
                    PRIMARY KEY (station_id, observation_day)
                )
                """);
            statement.executeUpdate("""
                CREATE INDEX IF NOT EXISTS noaa_daily_station_average_day_idx
                ON noaa_daily_station_average (observation_day)
                """);
            statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS noaa_yearly_station_rain_duration (
                    station_id text NOT NULL,
                    observation_year integer NOT NULL,
                    avg_duration_hours double precision NOT NULL,
                    sample_count bigint NOT NULL,
                    updated_at timestamptz NOT NULL DEFAULT now(),
                    PRIMARY KEY (station_id, observation_year)
                )
                """);
            statement.executeUpdate("""
                CREATE INDEX IF NOT EXISTS noaa_yearly_station_rain_duration_year_idx
                ON noaa_yearly_station_rain_duration (observation_year)
                """);
            statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS noaa_monthly_station_frost_days (
                    station_id text NOT NULL,
                    observation_month date NOT NULL,
                    frost_day_count bigint NOT NULL,
                    updated_at timestamptz NOT NULL DEFAULT now(),
                    PRIMARY KEY (station_id, observation_month)
                )
                """);
            statement.executeUpdate("""
                CREATE INDEX IF NOT EXISTS noaa_monthly_station_frost_days_month_idx
                ON noaa_monthly_station_frost_days (observation_month)
                """);
            statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS noaa_temperature_window_ranking (
                    window_start timestamptz NOT NULL,
                    window_end timestamptz NOT NULL,
                    ranking_type text NOT NULL,
                    rank_position integer NOT NULL,
                    station_id text NOT NULL,
                    peak_temperature_celsius double precision,
                    avg_temperature_celsius double precision NOT NULL,
                    sample_count bigint NOT NULL,
                    min_temperature_celsius double precision NOT NULL,
                    max_temperature_celsius double precision NOT NULL,
                    updated_at timestamptz NOT NULL DEFAULT now(),
                    PRIMARY KEY (window_start, window_end, ranking_type, rank_position)
                )
                """);
            statement.executeUpdate("""
                CREATE INDEX IF NOT EXISTS noaa_temperature_window_ranking_end_idx
                ON noaa_temperature_window_ranking (window_end)
                """);
            statement.executeUpdate("""
                ALTER TABLE noaa_temperature_window_ranking
                ADD COLUMN IF NOT EXISTS peak_temperature_celsius double precision
                """);
            statement.executeUpdate("""
                UPDATE noaa_temperature_window_ranking
                SET peak_temperature_celsius = CASE
                    WHEN ranking_type = 'hot' THEN max_temperature_celsius
                    ELSE min_temperature_celsius
                END
                WHERE peak_temperature_celsius IS NULL
                """);
            statement.executeUpdate("DELETE FROM noaa_temperature_window_ranking");
        }
    }

    private void insertRankingRows(
        Timestamp windowStart,
        Timestamp windowEnd,
        String rankingType,
        java.util.List<TemperatureRankingAggregate.RankedStation> stations,
        Timestamp updatedAt
    ) throws SQLException {
        int rankPosition = 1;
        for (TemperatureRankingAggregate.RankedStation station : stations) {
            double peakTemperature = "hot".equals(rankingType)
                ? station.maxTemperatureCelsius()
                : station.minTemperatureCelsius();
            insertDailyTemperatureRanking.setTimestamp(1, windowStart);
            insertDailyTemperatureRanking.setTimestamp(2, windowEnd);
            insertDailyTemperatureRanking.setString(3, rankingType);
            insertDailyTemperatureRanking.setInt(4, rankPosition++);
            insertDailyTemperatureRanking.setString(5, station.stationId());
            insertDailyTemperatureRanking.setDouble(6, peakTemperature);
            insertDailyTemperatureRanking.setDouble(7, station.averageTemperatureCelsius());
            insertDailyTemperatureRanking.setLong(8, station.count());
            insertDailyTemperatureRanking.setDouble(9, station.minTemperatureCelsius());
            insertDailyTemperatureRanking.setDouble(10, station.maxTemperatureCelsius());
            insertDailyTemperatureRanking.setTimestamp(11, updatedAt);
            insertDailyTemperatureRanking.executeUpdate();
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
