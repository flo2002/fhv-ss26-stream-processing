package fhv.streamprocessing.pattern4.tourism;

import static org.junit.jupiter.api.Assertions.assertEquals;

import fhv.streamprocessing.model.NoaaObservation;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class TourismWeatherQualityAggregateTest {
    @Test
    void calculatesCompositeScoreFromComfortableTemperatureAndLowWind() {
        TourismWeatherQualityAggregate aggregate = new TourismWeatherQualityAggregate();

        aggregate.add(observation(23.0, 3.0, 80.0), new WeatherRegion("NO", "Norway"));

        assertEquals(23.0, aggregate.avgTemperatureCelsius());
        assertEquals(3.0, aggregate.avgWindSpeedMetersPerSecond());
        assertEquals(80.0, aggregate.avgSkyClarityScore());
        assertEquals(90.1, aggregate.qualityScore());
        assertEquals("excellent", aggregate.qualityClass());
    }

    @Test
    void classifiesPoorWeatherWhenTemperatureIsUncomfortableAndWindIsStrong() {
        TourismWeatherQualityAggregate aggregate = new TourismWeatherQualityAggregate();

        aggregate.add(observation(2.0, 18.0, 10.0), new WeatherRegion("NO", "Norway"));

        assertEquals(11.3, aggregate.qualityScore());
        assertEquals("poor", aggregate.qualityClass());
    }

    private static NoaaObservation observation(double temperatureCelsius, double windMps, double skyClarityScore) {
        var timestamp = LocalDateTime.parse("2025-07-01T10:00:00").atOffset(ZoneOffset.UTC);
        return new NoaaObservation(
            "010010-99999",
            timestamp.toLocalDate(),
            timestamp,
            temperatureCelsius,
            "1",
            windMps,
            "1",
            skyClarityScore,
            "1",
            null,
            null,
            1,
            null
        );
    }
}
