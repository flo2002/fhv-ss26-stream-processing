package fhv.streamprocessing.pattern9.wetdry;

import java.time.Instant;

public record WetPeriodState(
    String stationId,
    Instant periodStart,
    long precipitationObservationCount
) {
    WetPeriodState addWetObservation() {
        return new WetPeriodState(stationId, periodStart, precipitationObservationCount + 1);
    }
}
