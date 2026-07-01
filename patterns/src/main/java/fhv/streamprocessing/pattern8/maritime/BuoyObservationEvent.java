package fhv.streamprocessing.pattern8.maritime;

import java.time.Instant;

/**
 * Marine buoy observation consumed by the maritime stream-stream join.
 */
public record BuoyObservationEvent(
    String buoyId,
    Instant observedAt,
    double latitude,
    double longitude,
    String seaAreaId,
    double waveHeightMeters,
    double windSpeedMetersPerSecond,
    double windDirectionDegrees
) {
}
