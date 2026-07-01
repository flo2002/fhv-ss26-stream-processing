package fhv.streamprocessing.pattern1.temperature;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;

/**
 * Writes daily average temperature results to dashboard tables using stable, replay-safe keys.
 */
public class DailyAverageTemperatureDashboardStore implements AutoCloseable {
    private final PreparedStatement upsertDailyAverage;

    public DailyAverageTemperatureDashboardStore(Connection connection) throws SQLException {
        createSchema(connection);
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
    }

    public void createSchema(Connection connection) throws SQLException {
        try (var statement = connection.createStatement()) {
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
        }
    }

    public void record(String stationDayKey, TemperatureAggregate aggregate) throws SQLException {
        StationDayKey stationDay = StationDayKey.parse(stationDayKey);
        upsertDailyAverage.setString(1, stationDay.stationId());
        upsertDailyAverage.setDate(2, Date.valueOf(stationDay.day()));
        upsertDailyAverage.setDouble(3, aggregate.averageTemperatureCelsius());
        upsertDailyAverage.setLong(4, aggregate.getCount());
        upsertDailyAverage.setTimestamp(5, Timestamp.from(OffsetDateTime.now().toInstant()));
        upsertDailyAverage.executeUpdate();
    }

    @Override
    public void close() throws SQLException {
        upsertDailyAverage.close();
    }
}
