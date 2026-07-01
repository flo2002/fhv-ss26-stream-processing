package fhv.streamprocessing.pattern10.blizzard;

/**
 * Immutable output event produced by the blizzard pattern.
 */
public record BlizzardEvent(
    String stationId,
    long windowStartEpochMs,
    long windowEndEpochMs,
    String eventType,
    double freezingThresholdCelsius,
    double strongWindThresholdMetersPerSecond,
    long sampleCount,
    long freezingObservationCount,
    long strongWindObservationCount,
    long precipitationObservationCount,
    double minTemperatureCelsius,
    double maxWindSpeedMetersPerSecond
) {
    public boolean isDetected() {
        // Cold and wind alone describe a storm; precipitation is also required
        // so that the event represents blizzard-like conditions.
        return freezingObservationCount > 0
            && strongWindObservationCount > 0
            && precipitationObservationCount > 0;
    }
}
