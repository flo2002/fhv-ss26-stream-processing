package fhv.streamprocessing.pattern4.tourism;

import java.util.Map;

/**
 * Resolves a station identifier to the region key used for Kafka grouping.
 */
public final class StationRegionResolver {
    private final Map<String, WeatherRegion> regionsByStationId;

    private StationRegionResolver(Map<String, WeatherRegion> regionsByStationId) {
        this.regionsByStationId = Map.copyOf(regionsByStationId);
    }

    public static StationRegionResolver fromMap(Map<String, WeatherRegion> regionsByStationId) {
        return new StationRegionResolver(regionsByStationId);
    }

    public static StationRegionResolver empty() {
        return new StationRegionResolver(Map.of());
    }

    public WeatherRegion resolve(String stationId) {
        if (stationId == null) {
            return WeatherRegion.UNKNOWN;
        }
        return regionsByStationId.getOrDefault(stationId, WeatherRegion.UNKNOWN);
    }

    public int size() {
        return regionsByStationId.size();
    }
}
