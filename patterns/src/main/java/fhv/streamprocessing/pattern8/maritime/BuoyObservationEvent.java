package fhv.streamprocessing.pattern8.maritime;

import java.time.Instant;

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
