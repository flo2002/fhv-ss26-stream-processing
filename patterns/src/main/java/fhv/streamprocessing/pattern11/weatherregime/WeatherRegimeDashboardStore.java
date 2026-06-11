package fhv.streamprocessing.pattern11.weatherregime;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;

public class WeatherRegimeDashboardStore implements AutoCloseable {
    private final PreparedStatement upsertWeatherRegime;

    public WeatherRegimeDashboardStore(Connection connection) throws SQLException {
        createSchema(connection);
        upsertWeatherRegime = connection.prepareStatement("""
            INSERT INTO noaa_weather_regimes (
                station_id,
                observation_date,
                cluster_id,
                cluster_label,
                distance_to_center,
                avg_temperature_celsius,
                min_temperature_celsius,
                max_temperature_celsius,
                temperature_range_celsius,
                avg_wind_speed_mps,
                max_wind_speed_mps,
                sky_clarity_score,
                precipitation_observation_count,
                rain_duration_hours,
                observation_count,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (station_id, observation_date)
            DO UPDATE SET
                cluster_id = excluded.cluster_id,
                cluster_label = excluded.cluster_label,
                distance_to_center = excluded.distance_to_center,
                avg_temperature_celsius = excluded.avg_temperature_celsius,
                min_temperature_celsius = excluded.min_temperature_celsius,
                max_temperature_celsius = excluded.max_temperature_celsius,
                temperature_range_celsius = excluded.temperature_range_celsius,
                avg_wind_speed_mps = excluded.avg_wind_speed_mps,
                max_wind_speed_mps = excluded.max_wind_speed_mps,
                sky_clarity_score = excluded.sky_clarity_score,
                precipitation_observation_count = excluded.precipitation_observation_count,
                rain_duration_hours = excluded.rain_duration_hours,
                observation_count = excluded.observation_count,
                updated_at = excluded.updated_at
            """);
    }

    public void createSchema(Connection connection) throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS noaa_weather_regimes (
                    station_id text NOT NULL,
                    observation_date date NOT NULL,
                    cluster_id integer NOT NULL,
                    cluster_label text NOT NULL,
                    distance_to_center double precision NOT NULL,
                    avg_temperature_celsius double precision NOT NULL,
                    min_temperature_celsius double precision NOT NULL,
                    max_temperature_celsius double precision NOT NULL,
                    temperature_range_celsius double precision NOT NULL,
                    avg_wind_speed_mps double precision NOT NULL,
                    max_wind_speed_mps double precision NOT NULL,
                    sky_clarity_score double precision NOT NULL,
                    precipitation_observation_count bigint NOT NULL,
                    rain_duration_hours bigint NOT NULL,
                    observation_count bigint NOT NULL,
                    updated_at timestamptz NOT NULL DEFAULT now(),
                    PRIMARY KEY (station_id, observation_date)
                )
                """);
            statement.executeUpdate("""
                CREATE INDEX IF NOT EXISTS noaa_weather_regimes_cluster_date_idx
                ON noaa_weather_regimes (cluster_id, observation_date DESC)
                """);
        }
    }

    public void record(String stationDayKey, WeatherRegimeEvent event) throws SQLException {
        upsertWeatherRegime.setString(1, event.stationId());
        upsertWeatherRegime.setDate(2, Date.valueOf(event.date()));
        upsertWeatherRegime.setInt(3, event.clusterId());
        upsertWeatherRegime.setString(4, event.clusterLabel());
        upsertWeatherRegime.setDouble(5, event.distanceToCenter());
        upsertWeatherRegime.setDouble(6, event.avgTemperatureCelsius());
        upsertWeatherRegime.setDouble(7, event.minTemperatureCelsius());
        upsertWeatherRegime.setDouble(8, event.maxTemperatureCelsius());
        upsertWeatherRegime.setDouble(9, event.temperatureRangeCelsius());
        upsertWeatherRegime.setDouble(10, event.avgWindSpeedMetersPerSecond());
        upsertWeatherRegime.setDouble(11, event.maxWindSpeedMetersPerSecond());
        upsertWeatherRegime.setDouble(12, event.skyClarityScore());
        upsertWeatherRegime.setLong(13, event.precipitationObservationCount());
        upsertWeatherRegime.setLong(14, event.rainDurationHours());
        upsertWeatherRegime.setLong(15, event.observationCount());
        upsertWeatherRegime.setTimestamp(16, Timestamp.from(OffsetDateTime.now().toInstant()));
        upsertWeatherRegime.executeUpdate();
    }

    public void clearExistingRows() throws SQLException {
        try (var statement = upsertWeatherRegime.getConnection().createStatement()) {
            statement.executeUpdate("DELETE FROM noaa_weather_regimes");
        }
    }

    @Override
    public void close() throws SQLException {
        upsertWeatherRegime.close();
    }
}
