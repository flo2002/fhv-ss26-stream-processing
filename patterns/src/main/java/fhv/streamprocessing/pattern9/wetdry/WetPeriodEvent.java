package fhv.streamprocessing.pattern9.wetdry;

import java.time.Instant;

/**
 * Immutable output event produced by the wet period pattern.
 */
public record WetPeriodEvent(
    String stationId,
    Instant periodStart,
    Instant periodEnd,
    long durationMinutes,
    long precipitationObservationCount,
    Instant closingObservedAt
) {
}
