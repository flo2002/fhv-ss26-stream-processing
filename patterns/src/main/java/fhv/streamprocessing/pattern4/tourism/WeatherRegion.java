package fhv.streamprocessing.pattern4.tourism;

public record WeatherRegion(
    String id,
    String name
) {
    static final WeatherRegion UNKNOWN = new WeatherRegion("unknown", "Unknown region");
}
