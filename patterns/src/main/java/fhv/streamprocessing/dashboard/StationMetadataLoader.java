package fhv.streamprocessing.dashboard;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URLConnection;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;

/**
 * Loads NOAA station metadata used to enrich dashboard rows with human-readable station details.
 */
public final class StationMetadataLoader {
    public static final String DEFAULT_STATION_HISTORY_URL = "https://www.ncei.noaa.gov/pub/data/noaa/isd-history.txt";

    private StationMetadataLoader() {
    }

    static void loadInto(Connection connection, String stationHistoryUrl) {
        if (stationHistoryUrl == null || stationHistoryUrl.isBlank()) {
            return;
        }
        URI uri;
        try {
            uri = URI.create(stationHistoryUrl);
        } catch (IllegalArgumentException exception) {
            System.err.printf("Could not load NOAA station metadata from %s: %s%n", stationHistoryUrl, exception.getMessage());
            return;
        }

        String scheme = uri.getScheme();
        if (scheme != null && !scheme.equalsIgnoreCase("http") && !scheme.equalsIgnoreCase("https")) {
            try {
                URLConnection connectionHandle = uri.toURL().openConnection();
                connectionHandle.setConnectTimeout(20_000);
                connectionHandle.setReadTimeout(120_000);
                upsertStationMetadata(connection, connectionHandle.getInputStream());
            } catch (IOException | SQLException | IllegalArgumentException exception) {
                System.err.printf("Could not load NOAA station metadata from %s: %s%n", stationHistoryUrl, exception.getMessage());
            }
            return;
        }

        HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(20))
            .build();
        HttpRequest request = HttpRequest.newBuilder(uri)
            .timeout(Duration.ofSeconds(60))
            .GET()
            .build();

        try {
            HttpResponse<java.io.InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                System.err.printf("Could not load NOAA station metadata: HTTP %s from %s%n", response.statusCode(), stationHistoryUrl);
                return;
            }
            upsertStationMetadata(connection, response.body());
        } catch (IOException | InterruptedException | SQLException | IllegalArgumentException exception) {
            if (exception instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            System.err.printf("Could not load NOAA station metadata from %s: %s%n", stationHistoryUrl, exception.getMessage());
        }
    }

    private static void upsertStationMetadata(Connection connection, java.io.InputStream inputStream) throws IOException, SQLException {
        try (
            var reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            PreparedStatement upsert = connection.prepareStatement("""
                INSERT INTO noaa_station_metadata (
                    station_id,
                    station_location,
                    station_name,
                    country_code,
                    state_code,
                    icao_code,
                    latitude,
                    longitude,
                    elevation_meters,
                    period_begin,
                    period_end,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
                ON CONFLICT (station_id)
                DO UPDATE SET
                    station_location = excluded.station_location,
                    station_name = excluded.station_name,
                    country_code = excluded.country_code,
                    state_code = excluded.state_code,
                    icao_code = excluded.icao_code,
                    latitude = excluded.latitude,
                    longitude = excluded.longitude,
                    elevation_meters = excluded.elevation_meters,
                    period_begin = excluded.period_begin,
                    period_end = excluded.period_end,
                    updated_at = excluded.updated_at
                """)
        ) {
            int pending = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                Optional<StationMetadata> metadata = parseLine(line);
                if (metadata.isEmpty()) {
                    continue;
                }

                StationMetadata station = metadata.get();
                upsert.setString(1, station.stationId());
                upsert.setString(2, station.stationLocation());
                upsert.setString(3, station.stationName());
                upsert.setString(4, station.countryCode());
                upsert.setString(5, station.stateCode());
                upsert.setString(6, station.icaoCode());
                setDoubleOrNull(upsert, 7, station.latitude());
                setDoubleOrNull(upsert, 8, station.longitude());
                setDoubleOrNull(upsert, 9, station.elevationMeters());
                upsert.setString(10, station.periodBegin());
                upsert.setString(11, station.periodEnd());
                upsert.addBatch();

                pending++;
                if (pending >= 500) {
                    upsert.executeBatch();
                    pending = 0;
                }
            }
            if (pending > 0) {
                upsert.executeBatch();
            }
        }
    }

    static Optional<StationMetadata> parseLine(String line) {
        if (line == null || line.length() < 43 || line.startsWith("USAF") || line.startsWith("----")) {
            return Optional.empty();
        }

        String usaf = clean(slice(line, 0, 6));
        String wban = clean(slice(line, 7, 12));
        String stationName = clean(slice(line, 13, 43));
        if (!isStationIdPart(usaf, 6) || !isStationIdPart(wban, 5) || stationName == null) {
            return Optional.empty();
        }

        String countryCode = clean(slice(line, 43, 48));
        String stateCode = clean(slice(line, 48, 51));
        String icaoCode = clean(slice(line, 51, 57));
        Double latitude = parseDouble(slice(line, 57, 65));
        Double longitude = parseDouble(slice(line, 65, 74));
        Double elevationMeters = parseDouble(slice(line, 74, 82));
        String periodBegin = clean(slice(line, 82, 91));
        String periodEnd = clean(slice(line, 91, 100));
        String stationLocation = stationName.equalsIgnoreCase("BOGUS") ? null : stationName;

        return Optional.of(new StationMetadata(
            usaf + "-" + wban,
            stationLocation,
            stationName,
            countryCode,
            stateCode,
            icaoCode,
            latitude,
            longitude,
            elevationMeters,
            periodBegin,
            periodEnd
        ));
    }

    private static void setDoubleOrNull(PreparedStatement statement, int index, Double value) throws SQLException {
        if (value == null) {
            statement.setNull(index, java.sql.Types.DOUBLE);
        } else {
            statement.setDouble(index, value);
        }
    }

    private static String slice(String value, int start, int end) {
        if (value.length() <= start) {
            return "";
        }
        return value.substring(start, Math.min(end, value.length()));
    }

    private static String clean(String value) {
        String cleaned = value == null ? "" : value.trim().replaceAll("\\s+", " ");
        return cleaned.isEmpty() ? null : cleaned;
    }

    private static Double parseDouble(String value) {
        String cleaned = clean(value);
        if (cleaned == null || cleaned.equals("+999.9") || cleaned.equals("-999.9")) {
            return null;
        }
        try {
            return Double.parseDouble(cleaned);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static boolean isStationIdPart(String value, int length) {
        return value != null && value.length() == length && value.matches("[A-Z0-9]+");
    }

    record StationMetadata(
        String stationId,
        String stationLocation,
        String stationName,
        String countryCode,
        String stateCode,
        String icaoCode,
        Double latitude,
        Double longitude,
        Double elevationMeters,
        String periodBegin,
        String periodEnd
    ) {
    }
}
