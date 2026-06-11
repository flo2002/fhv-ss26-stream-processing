package fhv.streamprocessing.pattern11.weatherregime;

public record WeatherRegimeAssignment(
    int clusterId,
    String clusterLabel,
    double distanceToCenter
) {
}
