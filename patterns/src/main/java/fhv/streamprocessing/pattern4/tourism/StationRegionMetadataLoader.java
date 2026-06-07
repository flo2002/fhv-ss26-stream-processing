package fhv.streamprocessing.pattern4.tourism;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public final class StationRegionMetadataLoader {
    private StationRegionMetadataLoader() {
    }

    public static StationRegionResolver loadFromUrl(String stationHistoryUrl) {
        if (stationHistoryUrl == null || stationHistoryUrl.isBlank()) {
            return StationRegionResolver.empty();
        }

        HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(20))
            .build();
        HttpRequest request = HttpRequest.newBuilder(URI.create(stationHistoryUrl))
            .timeout(Duration.ofSeconds(60))
            .GET()
            .build();

        try {
            HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                System.err.printf("Could not load station regions: HTTP %s from %s%n", response.statusCode(), stationHistoryUrl);
                return StationRegionResolver.empty();
            }
            return load(response.body());
        } catch (IOException | InterruptedException | IllegalArgumentException exception) {
            if (exception instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            System.err.printf("Could not load station regions from %s: %s%n", stationHistoryUrl, exception.getMessage());
            return StationRegionResolver.empty();
        }
    }

    static StationRegionResolver load(InputStream inputStream) {
        Map<String, WeatherRegion> regionsByStationId = new HashMap<>();
        try (var reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                StationRegionRow row = parseLine(line);
                if (row == null) {
                    continue;
                }
                regionsByStationId.put(row.stationId(), row.region());
            }
        } catch (IOException exception) {
            System.err.printf("Could not parse station regions: %s%n", exception.getMessage());
        }
        return StationRegionResolver.fromMap(regionsByStationId);
    }

    private static StationRegionRow parseLine(String line) {
        if (line == null || line.length() < 51 || line.startsWith("USAF") || line.startsWith("----")) {
            return null;
        }

        String usaf = clean(slice(line, 0, 6));
        String wban = clean(slice(line, 7, 12));
        String countryCode = clean(slice(line, 43, 48));
        String stateCode = clean(slice(line, 48, 51));
        if (usaf == null || wban == null || countryCode == null) {
            return null;
        }

        String regionId = stateCode == null ? countryCode : countryCode + "-" + stateCode;
        String regionName = stateCode == null ? countryName(countryCode) : countryName(countryCode) + " / " + stateCode;
        return new StationRegionRow(usaf + "-" + wban, new WeatherRegion(regionId, regionName));
    }

    private static String countryName(String countryCode) {
        if (countryCode.length() == 2) {
            String displayCountry = new Locale.Builder()
                .setRegion(countryCode)
                .build()
                .getDisplayCountry(Locale.ENGLISH);
            if (!displayCountry.isBlank()) {
                return displayCountry;
            }
        }
        return countryCode;
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

    private record StationRegionRow(String stationId, WeatherRegion region) {
    }
}
