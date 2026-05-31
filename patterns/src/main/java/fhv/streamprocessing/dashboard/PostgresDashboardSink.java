package fhv.streamprocessing.dashboard;

import fhv.streamprocessing.model.RainDurationAggregate;
import fhv.streamprocessing.model.TemperatureAggregate;
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

    public PostgresDashboardSink(String jdbcUrl, String user, String password) {
        try {
            connection = connectWithRetry(jdbcUrl, user, password);
            connection.setAutoCommit(true);
            createTables();
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
    public synchronized void close() {
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
