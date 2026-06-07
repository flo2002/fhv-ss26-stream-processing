package fhv.streamprocessing.pattern3.rapidchange;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;

public class RapidTemperatureChangeDashboardStore implements AutoCloseable {
    private final Connection connection;
    private final PreparedStatement upsertRapidChangeEvent;

    public RapidTemperatureChangeDashboardStore(Connection connection) throws SQLException {
        this.connection = connection;
        createSchema(connection);
        upsertRapidChangeEvent = connection.prepareStatement("""
            INSERT INTO noaa_rapid_temperature_change (
                station_id,
                window_start,
                window_end,
                min_rate_of_change,
                max_rate_of_change,
                average_rate_of_change,
                rate_of_change_count,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (station_id, window_start, window_end)
            DO UPDATE SET
                min_rate_of_change = excluded.min_rate_of_change,
                max_rate_of_change = excluded.max_rate_of_change,
                average_rate_of_change = excluded.average_rate_of_change,
                rate_of_change_count = excluded.rate_of_change_count,
                updated_at = excluded.updated_at
            """);
    }

    public void createSchema(Connection connection) throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS noaa_rapid_temperature_change (
                    station_id text NOT NULL,
                    window_start timestamptz NOT NULL,
                    window_end timestamptz NOT NULL,
                    min_rate_of_change double precision NOT NULL,
                    max_rate_of_change double precision NOT NULL,
                    average_rate_of_change double precision NOT NULL,
                    rate_of_change_count bigint NOT NULL,
                    updated_at timestamptz NOT NULL DEFAULT now(),
                    PRIMARY KEY (station_id, window_start, window_end)
                )
                """);
            statement.executeUpdate("""
                CREATE INDEX IF NOT EXISTS noaa_rapid_change_window_end_idx
                ON noaa_rapid_temperature_change (window_end DESC)
                """);
            statement.executeUpdate("""
                CREATE INDEX IF NOT EXISTS noaa_rapid_change_station_idx
                ON noaa_rapid_temperature_change (station_id)
                """);
        }
    }

    public void clearExistingRows() throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM noaa_rapid_temperature_change");
        }
    }

    public void record(String stationWindowKey, RapidTemperatureChangeEvent event) throws SQLException {
        StationRapidChangeWindowKey parsedKey = StationRapidChangeWindowKey.parse(stationWindowKey);
        Timestamp updatedAt = Timestamp.from(OffsetDateTime.now().toInstant());

        upsertRapidChangeEvent.setString(1, parsedKey.stationId());
        upsertRapidChangeEvent.setTimestamp(2, Timestamp.from(parsedKey.windowStart()));
        upsertRapidChangeEvent.setTimestamp(3, Timestamp.from(parsedKey.windowEnd()));
        upsertRapidChangeEvent.setDouble(4, event.minRateOfChange());
        upsertRapidChangeEvent.setDouble(5, event.maxRateOfChange());
        upsertRapidChangeEvent.setDouble(6, event.averageRateOfChange());
        upsertRapidChangeEvent.setLong(7, event.rateOfChangeCount());
        upsertRapidChangeEvent.setTimestamp(8, updatedAt);
        upsertRapidChangeEvent.executeUpdate();
    }

    @Override
    public void close() throws SQLException {
        upsertRapidChangeEvent.close();
    }
}
