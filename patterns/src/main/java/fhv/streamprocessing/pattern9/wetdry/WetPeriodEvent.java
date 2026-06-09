package fhv.streamprocessing.pattern9.wetdry;

import java.time.Instant;

public record WetPeriodEvent(
    String stationId,
    Instant periodStart,
    Instant periodEnd,
    long durationMinutes,
    long precipitationObservationCount,
    Instant closingObservedAt
) {
}
