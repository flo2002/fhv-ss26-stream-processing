package fhv.streamprocessing.pattern4.tourism;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;

public class TourismWeatherQualityDashboardStore implements AutoCloseable {
    private final PreparedStatement upsertTourismQuality;

    public TourismWeatherQualityDashboardStore(Connection connection) throws SQLException {
        createSchema(connection);
        upsertTourismQuality = connection.prepareStatement("""
            INSERT INTO noaa_tourism_weather_quality (
                region_id,
                region_name,
                window_start,
                window_end,
                avg_temperature_celsius,
                avg_wind_speed_mps,
                sky_clarity_score,
                quality_score,
                quality_class,
                observation_count,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (region_id, window_start, window_end)
            DO UPDATE SET
                region_name = excluded.region_name,
                avg_temperature_celsius = excluded.avg_temperature_celsius,
                avg_wind_speed_mps = excluded.avg_wind_speed_mps,
                sky_clarity_score = excluded.sky_clarity_score,
                quality_score = excluded.quality_score,
                quality_class = excluded.quality_class,
                observation_count = excluded.observation_count,
                updated_at = excluded.updated_at
            """);
    }

    public void createSchema(Connection connection) throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS noaa_tourism_weather_quality (
                    region_id text NOT NULL,
                    region_name text NOT NULL,
                    window_start timestamptz NOT NULL,
                    window_end timestamptz NOT NULL,
                    avg_temperature_celsius double precision NOT NULL,
                    avg_wind_speed_mps double precision NOT NULL,
                    sky_clarity_score double precision NOT NULL,
                    quality_score double precision NOT NULL,
                    quality_class text NOT NULL,
                    observation_count bigint NOT NULL,
                    updated_at timestamptz NOT NULL DEFAULT now(),
                    PRIMARY KEY (region_id, window_start, window_end)
                )
                """);
            statement.executeUpdate("""
                CREATE INDEX IF NOT EXISTS noaa_tourism_weather_quality_window_idx
                ON noaa_tourism_weather_quality (window_end DESC)
                """);
            statement.executeUpdate("""
                ALTER TABLE noaa_tourism_weather_quality
                ADD COLUMN IF NOT EXISTS sky_clarity_score double precision NOT NULL DEFAULT 0
                """);
            statement.executeUpdate("""
                ALTER TABLE noaa_tourism_weather_quality
                DROP COLUMN IF EXISTS sunshine_score
                """);
        }
    }

    public void record(String regionWindowKey, TourismWeatherQualityEvent event) throws SQLException {
        RegionWindowKey.parse(regionWindowKey);
        upsertTourismQuality.setString(1, event.regionId());
        upsertTourismQuality.setString(2, event.regionName());
        upsertTourismQuality.setTimestamp(3, Timestamp.from(event.windowStart()));
        upsertTourismQuality.setTimestamp(4, Timestamp.from(event.windowEnd()));
        upsertTourismQuality.setDouble(5, event.avgTemperatureCelsius());
        upsertTourismQuality.setDouble(6, event.avgWindSpeedMetersPerSecond());
        upsertTourismQuality.setDouble(7, event.skyClarityScore());
        upsertTourismQuality.setDouble(8, event.qualityScore());
        upsertTourismQuality.setString(9, event.qualityClass());
        upsertTourismQuality.setLong(10, event.observationCount());
        upsertTourismQuality.setTimestamp(11, Timestamp.from(OffsetDateTime.now().toInstant()));
        upsertTourismQuality.executeUpdate();
    }

    public void clearExistingRows() throws SQLException {
        try (var statement = upsertTourismQuality.getConnection().createStatement()) {
            statement.executeUpdate("DELETE FROM noaa_tourism_weather_quality");
        }
    }

    @Override
    public void close() throws SQLException {
        upsertTourismQuality.close();
    }
}
