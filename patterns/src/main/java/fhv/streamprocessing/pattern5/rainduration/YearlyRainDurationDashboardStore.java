package fhv.streamprocessing.pattern5.rainduration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;

/**
 * Writes yearly rain duration results to dashboard tables using stable, replay-safe keys.
 */
public class YearlyRainDurationDashboardStore implements AutoCloseable {
    private final PreparedStatement upsertYearlyRainDuration;

    public YearlyRainDurationDashboardStore(Connection connection) throws SQLException {
        createSchema(connection);
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
    }

    public void createSchema(Connection connection) throws SQLException {
        try (var statement = connection.createStatement()) {
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

    public void record(String stationYearKey, RainDurationAggregate aggregate) throws SQLException {
        StationYearKey stationYear = StationYearKey.parse(stationYearKey);
        upsertYearlyRainDuration.setString(1, stationYear.stationId());
        upsertYearlyRainDuration.setInt(2, stationYear.year());
        upsertYearlyRainDuration.setDouble(3, aggregate.averageDurationHours());
        upsertYearlyRainDuration.setLong(4, aggregate.getCount());
        upsertYearlyRainDuration.setTimestamp(5, Timestamp.from(OffsetDateTime.now().toInstant()));
        upsertYearlyRainDuration.executeUpdate();
    }

    @Override
    public void close() throws SQLException {
        upsertYearlyRainDuration.close();
    }
}
