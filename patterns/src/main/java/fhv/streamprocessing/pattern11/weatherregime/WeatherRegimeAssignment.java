package fhv.streamprocessing.pattern11.weatherregime;

/**
 * Result of assigning one feature vector to a learned weather-regime cluster.
 */
public record WeatherRegimeAssignment(
    int clusterId,
    String clusterLabel,
    double distanceToCenter
) {
}
