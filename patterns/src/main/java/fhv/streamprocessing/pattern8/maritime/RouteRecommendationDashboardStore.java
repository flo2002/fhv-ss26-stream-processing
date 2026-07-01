package fhv.streamprocessing.pattern8.maritime;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * Writes route recommendation results to dashboard tables using stable, replay-safe keys.
 */
public class RouteRecommendationDashboardStore implements AutoCloseable {
    private final Connection connection;
    private final PreparedStatement upsert;

    public RouteRecommendationDashboardStore(Connection connection) throws SQLException {
        this.connection = connection;
        createTable();
        upsert = connection.prepareStatement("""
            INSERT INTO marine_route_recommendations (
                event_key,
                mmsi,
                vessel_name,
                sea_area_id,
                window_start,
                window_end,
                observed_at,
                latitude,
                longitude,
                speed_over_ground_knots,
                course_over_ground_degrees,
                wave_height_meters,
                wind_speed_mps,
                risk_score,
                risk_class,
                recommendation,
                reported_eta,
                updated_eta,
                eta_delay_minutes,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
            ON CONFLICT (event_key)
            DO UPDATE SET
                mmsi = EXCLUDED.mmsi,
                vessel_name = EXCLUDED.vessel_name,
                sea_area_id = EXCLUDED.sea_area_id,
                window_start = EXCLUDED.window_start,
                window_end = EXCLUDED.window_end,
                observed_at = EXCLUDED.observed_at,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                speed_over_ground_knots = EXCLUDED.speed_over_ground_knots,
                course_over_ground_degrees = EXCLUDED.course_over_ground_degrees,
                wave_height_meters = EXCLUDED.wave_height_meters,
                wind_speed_mps = EXCLUDED.wind_speed_mps,
                risk_score = EXCLUDED.risk_score,
                risk_class = EXCLUDED.risk_class,
                recommendation = EXCLUDED.recommendation,
                reported_eta = EXCLUDED.reported_eta,
                updated_eta = EXCLUDED.updated_eta,
                eta_delay_minutes = EXCLUDED.eta_delay_minutes,
                updated_at = now()
            """);
    }

    public void clearExistingRows() throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.executeUpdate("TRUNCATE TABLE marine_route_recommendations");
        }
    }

    public void record(String eventKey, RouteRecommendationEvent event) throws SQLException {
        upsert.setString(1, eventKey);
        upsert.setString(2, event.mmsi());
        upsert.setString(3, event.vesselName());
        upsert.setString(4, event.seaAreaId());
        upsert.setTimestamp(5, Timestamp.from(event.windowStart()));
        upsert.setTimestamp(6, Timestamp.from(event.windowEnd()));
        upsert.setTimestamp(7, Timestamp.from(event.observedAt()));
        upsert.setDouble(8, event.latitude());
        upsert.setDouble(9, event.longitude());
        upsert.setDouble(10, event.speedOverGroundKnots());
        upsert.setDouble(11, event.courseOverGroundDegrees());
        upsert.setDouble(12, event.waveHeightMeters());
        upsert.setDouble(13, event.windSpeedMetersPerSecond());
        upsert.setDouble(14, event.riskScore());
        upsert.setString(15, event.riskClass());
        upsert.setString(16, event.recommendation());
        upsert.setTimestamp(17, event.reportedEta() == null ? null : Timestamp.from(event.reportedEta()));
        upsert.setTimestamp(18, event.updatedEta() == null ? null : Timestamp.from(event.updatedEta()));
        upsert.setLong(19, event.etaDelayMinutes());
        upsert.executeUpdate();
    }

    private void createTable() throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS marine_route_recommendations (
                    event_key text PRIMARY KEY,
                    mmsi text NOT NULL,
                    vessel_name text,
                    sea_area_id text NOT NULL,
                    window_start timestamptz NOT NULL,
                    window_end timestamptz NOT NULL,
                    observed_at timestamptz NOT NULL,
                    latitude double precision NOT NULL,
                    longitude double precision NOT NULL,
                    speed_over_ground_knots double precision NOT NULL,
                    course_over_ground_degrees double precision NOT NULL,
                    wave_height_meters double precision NOT NULL,
                    wind_speed_mps double precision NOT NULL,
                    risk_score double precision NOT NULL,
                    risk_class text NOT NULL,
                    recommendation text NOT NULL,
                    reported_eta timestamptz,
                    updated_eta timestamptz,
                    eta_delay_minutes bigint NOT NULL,
                    updated_at timestamptz NOT NULL DEFAULT now()
                )
                """);
        }
    }

    @Override
    public void close() throws SQLException {
        upsert.close();
    }
}
