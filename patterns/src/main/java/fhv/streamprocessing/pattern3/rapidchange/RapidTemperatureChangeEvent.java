package fhv.streamprocessing.pattern3.rapidchange;

import java.time.Instant;

public record RapidTemperatureChangeEvent(
    String stationId,
    Instant windowStart,
    Instant windowEnd,
    double minRateOfChange,
    double maxRateOfChange,
    double averageRateOfChange,
    long rateOfChangeCount
) {
}
