package fhv.streamprocessing.pattern8.maritime;

import java.time.Instant;

/**
 * Immutable output event produced by the route recommendation pattern.
 */
public record RouteRecommendationEvent(
    String mmsi,
    String vesselName,
    String seaAreaId,
    Instant windowStart,
    Instant windowEnd,
    Instant observedAt,
    double latitude,
    double longitude,
    double speedOverGroundKnots,
    double courseOverGroundDegrees,
    double waveHeightMeters,
    double windSpeedMetersPerSecond,
    double riskScore,
    String riskClass,
    String recommendation,
    Instant reportedEta,
    Instant updatedEta,
    long etaDelayMinutes
) {
}
