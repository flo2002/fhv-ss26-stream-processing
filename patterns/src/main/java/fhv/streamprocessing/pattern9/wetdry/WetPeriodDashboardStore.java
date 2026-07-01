package fhv.streamprocessing.pattern9.wetdry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * Writes wet period results to dashboard tables using stable, replay-safe keys.
 */
public class WetPeriodDashboardStore implements AutoCloseable {
    private final Connection connection;
    private final PreparedStatement upsertWetPeriod;

    public WetPeriodDashboardStore(Connection connection) throws SQLException {
        this.connection = connection;
        createTable();
        upsertWetPeriod = connection.prepareStatement("""
            INSERT INTO noaa_wet_period_events (
                event_key,
                station_id,
                period_start,
                period_end,
                duration_minutes,
                precipitation_observation_count,
                closing_observed_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, now())
            ON CONFLICT (event_key)
            DO UPDATE SET
                station_id = EXCLUDED.station_id,
                period_start = EXCLUDED.period_start,
                period_end = EXCLUDED.period_end,
                duration_minutes = EXCLUDED.duration_minutes,
                precipitation_observation_count = EXCLUDED.precipitation_observation_count,
                closing_observed_at = EXCLUDED.closing_observed_at,
                updated_at = now()
            """);
    }

    public void clearExistingRows() throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.executeUpdate("TRUNCATE TABLE noaa_wet_period_events");
        }
    }

    public void record(String eventKey, WetPeriodEvent event) throws SQLException {
        upsertWetPeriod.setString(1, eventKey);
        upsertWetPeriod.setString(2, event.stationId());
        upsertWetPeriod.setTimestamp(3, Timestamp.from(event.periodStart()));
        upsertWetPeriod.setTimestamp(4, Timestamp.from(event.periodEnd()));
        upsertWetPeriod.setLong(5, event.durationMinutes());
        upsertWetPeriod.setLong(6, event.precipitationObservationCount());
        upsertWetPeriod.setTimestamp(7, Timestamp.from(event.closingObservedAt()));
        upsertWetPeriod.executeUpdate();
    }

    private void createTable() throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS noaa_wet_period_events (
                    event_key text PRIMARY KEY,
                    station_id text NOT NULL,
                    period_start timestamptz NOT NULL,
                    period_end timestamptz NOT NULL,
                    duration_minutes bigint NOT NULL,
                    precipitation_observation_count bigint NOT NULL,
                    closing_observed_at timestamptz NOT NULL,
                    updated_at timestamptz NOT NULL DEFAULT now()
                )
                """);
        }
    }

    @Override
    public void close() throws SQLException {
        upsertWetPeriod.close();
    }
}
