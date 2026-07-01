package fhv.streamprocessing.pattern9.wetdry;

import java.time.Instant;

/**
 * Persistent per-station state representing a wet period that has not closed yet.
 */
public record WetPeriodState(
    String stationId,
    Instant periodStart,
    long precipitationObservationCount
) {
    WetPeriodState addWetObservation() {
        // Records are immutable: write a new value to Kafka Streams' state
        // store while keeping the original period start unchanged.
        return new WetPeriodState(stationId, periodStart, precipitationObservationCount + 1);
    }
}
