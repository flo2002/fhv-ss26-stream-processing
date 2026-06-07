package fhv.streamprocessing.pattern7.forecasting;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;

public class TemperatureForecastDashboardStore implements AutoCloseable {
    private final Connection connection;
    private final PreparedStatement upsertForecast;

    public TemperatureForecastDashboardStore(Connection connection) throws SQLException {
        this.connection = connection;
        createSchema(connection);
        upsertForecast = connection.prepareStatement("""
            INSERT INTO noaa_temperature_forecast (
                station_id,
                window_start,
                window_end,
                slope_per_second,
                current_average,
                forecast_next_24h,
                observation_count,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (station_id, window_start, window_end)
            DO UPDATE SET
                slope_per_second = excluded.slope_per_second,
                current_average = excluded.current_average,
                forecast_next_24h = excluded.forecast_next_24h,
                observation_count = excluded.observation_count,
                updated_at = excluded.updated_at
            """);
    }

    public void createSchema(Connection connection) throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS noaa_temperature_forecast (
                    station_id text NOT NULL,
                    window_start timestamptz NOT NULL,
                    window_end timestamptz NOT NULL,
                    slope_per_second double precision NOT NULL,
                    current_average double precision NOT NULL,
                    forecast_next_24h double precision NOT NULL,
                    observation_count bigint NOT NULL,
                    updated_at timestamptz NOT NULL DEFAULT now(),
                    PRIMARY KEY (station_id, window_start, window_end)
                )
                """);
            statement.executeUpdate("""
                CREATE INDEX IF NOT EXISTS noaa_forecast_window_end_idx
                ON noaa_temperature_forecast (window_end DESC)
                """);
            statement.executeUpdate("""
                CREATE INDEX IF NOT EXISTS noaa_forecast_station_idx
                ON noaa_temperature_forecast (station_id)
                """);
        }
    }

    public void clearExistingRows() throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM noaa_temperature_forecast");
        }
    }

    public void record(String stationForecastKey, TemperatureForecastEvent event) throws SQLException {
        StationForecastKey parsedKey = StationForecastKey.parse(stationForecastKey);
        Timestamp updatedAt = Timestamp.from(OffsetDateTime.now().toInstant());

        upsertForecast.setString(1, parsedKey.stationId());
        upsertForecast.setTimestamp(2, Timestamp.from(parsedKey.windowStart()));
        upsertForecast.setTimestamp(3, Timestamp.from(parsedKey.windowEnd()));
        upsertForecast.setDouble(4, event.slopePerSecond());
        upsertForecast.setDouble(5, event.currentAverage());
        upsertForecast.setDouble(6, event.forecastNext24h());
        upsertForecast.setLong(7, event.observationCount());
        upsertForecast.setTimestamp(8, updatedAt);
        upsertForecast.executeUpdate();
    }

    @Override
    public void close() throws SQLException {
        upsertForecast.close();
    }
}
