package fhv.streamprocessing.pattern2.frostdays;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;

public class MonthlyFrostDaysDashboardStore implements AutoCloseable {
    private final PreparedStatement upsertMonthlyFrostDays;

    public MonthlyFrostDaysDashboardStore(Connection connection) throws SQLException {
        createSchema(connection);
        upsertMonthlyFrostDays = connection.prepareStatement("""
            INSERT INTO noaa_monthly_station_frost_days (
                station_id,
                observation_month,
                frost_day_count,
                updated_at
            )
            VALUES (?, ?, ?, ?)
            ON CONFLICT (station_id, observation_month)
            DO UPDATE SET
                frost_day_count = excluded.frost_day_count,
                updated_at = excluded.updated_at
            """);
    }

    public void createSchema(Connection connection) throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS noaa_monthly_station_frost_days (
                    station_id text NOT NULL,
                    observation_month date NOT NULL,
                    frost_day_count bigint NOT NULL,
                    updated_at timestamptz NOT NULL DEFAULT now(),
                    PRIMARY KEY (station_id, observation_month)
                )
                """);
            statement.executeUpdate("""
                CREATE INDEX IF NOT EXISTS noaa_monthly_station_frost_days_month_idx
                ON noaa_monthly_station_frost_days (observation_month)
                """);
        }
    }

    public void record(String stationMonthKey, Long frostDayCount) throws SQLException {
        StationMonthKey stationMonth = StationMonthKey.parse(stationMonthKey);
        upsertMonthlyFrostDays.setString(1, stationMonth.stationId());
        upsertMonthlyFrostDays.setDate(2, Date.valueOf(stationMonth.month().atDay(1)));
        upsertMonthlyFrostDays.setLong(3, frostDayCount);
        upsertMonthlyFrostDays.setTimestamp(4, Timestamp.from(OffsetDateTime.now().toInstant()));
        upsertMonthlyFrostDays.executeUpdate();
    }

    @Override
    public void close() throws SQLException {
        upsertMonthlyFrostDays.close();
    }
}
