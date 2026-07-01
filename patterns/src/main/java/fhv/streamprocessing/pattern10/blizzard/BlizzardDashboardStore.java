package fhv.streamprocessing.pattern10.blizzard;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;

/**
 * Writes blizzard results to dashboard tables using stable, replay-safe keys.
 */
public class BlizzardDashboardStore implements AutoCloseable {
    private final PreparedStatement deleteBlizzardEvents;
    private final PreparedStatement upsertBlizzardEvent;

    public BlizzardDashboardStore(Connection connection) throws SQLException {
        createSchema(connection);
        deleteBlizzardEvents = connection.prepareStatement("""
            DELETE FROM noaa_blizzard_events
            """);
        upsertBlizzardEvent = connection.prepareStatement("""
            INSERT INTO noaa_blizzard_events (
                station_id,
                window_start,
                window_end,
                event_type,
                freezing_threshold_celsius,
                wind_threshold_mps,
                sample_count,
                freezing_observation_count,
                strong_wind_observation_count,
                precipitation_observation_count,
                min_temperature_celsius,
                max_wind_speed_mps,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (station_id, window_start, window_end)
            DO UPDATE SET
                event_type = excluded.event_type,
                freezing_threshold_celsius = excluded.freezing_threshold_celsius,
                wind_threshold_mps = excluded.wind_threshold_mps,
                sample_count = excluded.sample_count,
                freezing_observation_count = excluded.freezing_observation_count,
                strong_wind_observation_count = excluded.strong_wind_observation_count,
                precipitation_observation_count = excluded.precipitation_observation_count,
                min_temperature_celsius = excluded.min_temperature_celsius,
                max_wind_speed_mps = excluded.max_wind_speed_mps,
                updated_at = excluded.updated_at
            """);
    }

    public void createSchema(Connection connection) throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS noaa_blizzard_events (
                    station_id text NOT NULL,
                    window_start timestamptz NOT NULL,
                    window_end timestamptz NOT NULL,
                    event_type text NOT NULL,
                    freezing_threshold_celsius double precision NOT NULL,
                    wind_threshold_mps double precision NOT NULL,
                    sample_count bigint NOT NULL,
                    freezing_observation_count bigint NOT NULL,
                    strong_wind_observation_count bigint NOT NULL,
                    precipitation_observation_count bigint NOT NULL,
                    min_temperature_celsius double precision NOT NULL,
                    max_wind_speed_mps double precision NOT NULL,
                    updated_at timestamptz NOT NULL DEFAULT now(),
                    PRIMARY KEY (station_id, window_start, window_end)
                )
                """);
            statement.executeUpdate("""
                CREATE INDEX IF NOT EXISTS noaa_blizzard_events_window_end_idx
                ON noaa_blizzard_events (window_end DESC)
                """);
            statement.executeUpdate("""
                CREATE INDEX IF NOT EXISTS noaa_blizzard_events_station_idx
                ON noaa_blizzard_events (station_id)
                """);
        }
    }

    public void clearExistingRows() throws SQLException {
        deleteBlizzardEvents.executeUpdate();
    }

    public void record(String stationWindowKey, BlizzardEvent event) throws SQLException {
        StationBlizzardWindowKey parsedKey = StationBlizzardWindowKey.parse(stationWindowKey);
        Timestamp updatedAt = Timestamp.from(OffsetDateTime.now().toInstant());

        upsertBlizzardEvent.setString(1, parsedKey.stationId());
        upsertBlizzardEvent.setTimestamp(2, Timestamp.from(parsedKey.windowStart()));
        upsertBlizzardEvent.setTimestamp(3, Timestamp.from(parsedKey.windowEnd()));
        upsertBlizzardEvent.setString(4, event.eventType());
        upsertBlizzardEvent.setDouble(5, event.freezingThresholdCelsius());
        upsertBlizzardEvent.setDouble(6, event.strongWindThresholdMetersPerSecond());
        upsertBlizzardEvent.setLong(7, event.sampleCount());
        upsertBlizzardEvent.setLong(8, event.freezingObservationCount());
        upsertBlizzardEvent.setLong(9, event.strongWindObservationCount());
        upsertBlizzardEvent.setLong(10, event.precipitationObservationCount());
        upsertBlizzardEvent.setDouble(11, event.minTemperatureCelsius());
        upsertBlizzardEvent.setDouble(12, event.maxWindSpeedMetersPerSecond());
        upsertBlizzardEvent.setTimestamp(13, updatedAt);
        upsertBlizzardEvent.executeUpdate();
    }

    @Override
    public void close() throws SQLException {
        upsertBlizzardEvent.close();
        deleteBlizzardEvents.close();
    }
}
