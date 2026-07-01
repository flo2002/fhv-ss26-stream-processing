package fhv.streamprocessing.pattern4.tourism;

/**
 * Region metadata attached to stations before tourism quality aggregation.
 */
public record WeatherRegion(
    String id,
    String name
) {
    static final WeatherRegion UNKNOWN = new WeatherRegion("unknown", "Unknown region");
}
