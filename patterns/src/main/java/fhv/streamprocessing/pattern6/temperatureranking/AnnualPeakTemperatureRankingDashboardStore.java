package fhv.streamprocessing.pattern6.temperatureranking;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.List;

/**
 * Writes annual peak temperature ranking results to dashboard tables using stable, replay-safe keys.
 */
public class AnnualPeakTemperatureRankingDashboardStore implements AutoCloseable {
    private final PreparedStatement deleteTemperatureRankings;
    private final PreparedStatement insertTemperatureRanking;

    public AnnualPeakTemperatureRankingDashboardStore(Connection connection) throws SQLException {
        createSchema(connection);
        deleteTemperatureRankings = connection.prepareStatement("""
            DELETE FROM noaa_temperature_window_ranking
            """);
        insertTemperatureRanking = connection.prepareStatement("""
            INSERT INTO noaa_temperature_window_ranking (
                window_start,
                window_end,
                ranking_type,
                rank_position,
                station_id,
                peak_temperature_celsius,
                avg_temperature_celsius,
                sample_count,
                min_temperature_celsius,
                max_temperature_celsius,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (window_start, window_end, ranking_type, rank_position)
            DO UPDATE SET
                station_id = excluded.station_id,
                peak_temperature_celsius = excluded.peak_temperature_celsius,
                avg_temperature_celsius = excluded.avg_temperature_celsius,
                sample_count = excluded.sample_count,
                min_temperature_celsius = excluded.min_temperature_celsius,
                max_temperature_celsius = excluded.max_temperature_celsius,
                updated_at = excluded.updated_at
            """);
    }

    public void createSchema(Connection connection) throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS noaa_temperature_window_ranking (
                    window_start timestamptz NOT NULL,
                    window_end timestamptz NOT NULL,
                    ranking_type text NOT NULL,
                    rank_position integer NOT NULL,
                    station_id text NOT NULL,
                    peak_temperature_celsius double precision,
                    avg_temperature_celsius double precision NOT NULL,
                    sample_count bigint NOT NULL,
                    min_temperature_celsius double precision NOT NULL,
                    max_temperature_celsius double precision NOT NULL,
                    updated_at timestamptz NOT NULL DEFAULT now(),
                    PRIMARY KEY (window_start, window_end, ranking_type, rank_position)
                )
                """);
            statement.executeUpdate("""
                CREATE INDEX IF NOT EXISTS noaa_temperature_window_ranking_end_idx
                ON noaa_temperature_window_ranking (window_end)
                """);
            statement.executeUpdate("""
                ALTER TABLE noaa_temperature_window_ranking
                ADD COLUMN IF NOT EXISTS peak_temperature_celsius double precision
                """);
            statement.executeUpdate("""
                UPDATE noaa_temperature_window_ranking
                SET peak_temperature_celsius = CASE
                    WHEN ranking_type = 'hot' THEN max_temperature_celsius
                    ELSE min_temperature_celsius
                END
                WHERE peak_temperature_celsius IS NULL
                """);
        }
    }

    public void clearExistingRows() throws SQLException {
        deleteTemperatureRankings.executeUpdate();
    }

    public void record(String rankingWindowKey, TemperatureRankingAggregate aggregate) throws SQLException {
        RankingWindowKey rankingWindow = RankingWindowKey.parse(rankingWindowKey);
        Timestamp windowStart = Timestamp.from(rankingWindow.windowStart());
        Timestamp windowEnd = Timestamp.from(rankingWindow.windowEnd());
        Timestamp updatedAt = Timestamp.from(OffsetDateTime.now().toInstant());

        insertRankingRows(windowStart, windowEnd, "hot", aggregate.hottestStations(10), updatedAt);
        insertRankingRows(windowStart, windowEnd, "cold", aggregate.coldestStations(10), updatedAt);
    }

    private void insertRankingRows(
        Timestamp windowStart,
        Timestamp windowEnd,
        String rankingType,
        List<TemperatureRankingAggregate.RankedStation> stations,
        Timestamp updatedAt
    ) throws SQLException {
        int rankPosition = 1;
        for (TemperatureRankingAggregate.RankedStation station : stations) {
            double peakTemperature = "hot".equals(rankingType)
                ? station.maxTemperatureCelsius()
                : station.minTemperatureCelsius();
            insertTemperatureRanking.setTimestamp(1, windowStart);
            insertTemperatureRanking.setTimestamp(2, windowEnd);
            insertTemperatureRanking.setString(3, rankingType);
            insertTemperatureRanking.setInt(4, rankPosition++);
            insertTemperatureRanking.setString(5, station.stationId());
            insertTemperatureRanking.setDouble(6, peakTemperature);
            insertTemperatureRanking.setDouble(7, station.averageTemperatureCelsius());
            insertTemperatureRanking.setLong(8, station.count());
            insertTemperatureRanking.setDouble(9, station.minTemperatureCelsius());
            insertTemperatureRanking.setDouble(10, station.maxTemperatureCelsius());
            insertTemperatureRanking.setTimestamp(11, updatedAt);
            insertTemperatureRanking.executeUpdate();
        }
    }

    @Override
    public void close() throws SQLException {
        insertTemperatureRanking.close();
        deleteTemperatureRankings.close();
    }
}
