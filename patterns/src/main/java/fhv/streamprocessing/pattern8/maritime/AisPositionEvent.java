package fhv.streamprocessing.pattern8.maritime;

import java.time.Instant;

public record AisPositionEvent(
    String mmsi,
    String vesselName,
    Instant observedAt,
    double latitude,
    double longitude,
    double speedOverGroundKnots,
    double courseOverGroundDegrees,
    String destination,
    Instant reportedEta,
    String seaAreaId
) {
}
