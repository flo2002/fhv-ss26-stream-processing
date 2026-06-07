package fhv.streamprocessing.serde;

import static org.junit.jupiter.api.Assertions.assertEquals;

import fhv.streamprocessing.model.NoaaObservation;
import fhv.streamprocessing.pattern1.temperature.TemperatureAggregate;
import fhv.streamprocessing.pattern5.rainduration.RainDurationAggregate;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import org.junit.jupiter.api.Test;

class JsonSerdeTest {
    @Test
    void roundTripsObservationWithoutComputedProperties() {
        NoaaObservation observation = new NoaaObservation(
            "010010-99999",
            LocalDate.parse("2025-03-27"),
            OffsetDateTime.parse("2025-03-27T11:00:00Z"),
            -6.5,
            "1",
            10.4,
            "1",
            null,
            null,
            null,
            "/pub/data/noaa/2025/010010-99999-2025.gz",
            751,
            "raw"
        );
        JsonSerde<NoaaObservation> serde = new JsonSerde<>(NoaaObservation.class);

        byte[] bytes = serde.serializer().serialize("test", observation);
        String json = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        NoaaObservation decoded = serde.deserializer().deserialize("test", bytes);

        assertEquals(observation, decoded);
        assertEquals(-1, json.indexOf("usableForTemperatureAverages"));
        assertEquals(-1, json.indexOf("stationDayKey"));
    }

    @Test
    void toleratesObservationJsonWrittenBeforeComputedPropertiesWereIgnored() {
        String json = """
            {
              "stationId": "010010-99999",
              "observationDate": "2025-03-27",
              "observedAt": "2025-03-27T11:00:00Z",
              "temperatureCelsius": -6.5,
              "temperatureQualityCode": "1",
              "sourcePath": "/pub/data/noaa/2025/010010-99999-2025.gz",
              "recordNumber": 751,
              "rawPayload": "raw",
              "usableForTemperatureAverages": true
            }
            """;
        JsonSerde<NoaaObservation> serde = new JsonSerde<>(NoaaObservation.class);

        NoaaObservation decoded = serde.deserializer().deserialize("test", json.getBytes(java.nio.charset.StandardCharsets.UTF_8));

        assertEquals("010010-99999", decoded.stationId());
    }

    @Test
    void roundTripsAggregateWithoutComputedAverageProperty() {
        TemperatureAggregate aggregate = new TemperatureAggregate().add(2.0).add(4.0);
        JsonSerde<TemperatureAggregate> serde = new JsonSerde<>(TemperatureAggregate.class);

        byte[] bytes = serde.serializer().serialize("test", aggregate);
        String json = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        TemperatureAggregate decoded = serde.deserializer().deserialize("test", bytes);

        assertEquals(2, decoded.getCount());
        assertEquals(6.0, decoded.getSum());
        assertEquals(3.0, decoded.averageTemperatureCelsius());
        assertEquals(-1, json.indexOf("averageTemperatureCelsius"));
    }

    @Test
    void roundTripsRainDurationAggregateWithoutComputedAverageProperty() {
        RainDurationAggregate aggregate = new RainDurationAggregate().add(6).add(12);
        JsonSerde<RainDurationAggregate> serde = new JsonSerde<>(RainDurationAggregate.class);

        byte[] bytes = serde.serializer().serialize("test", aggregate);
        String json = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        RainDurationAggregate decoded = serde.deserializer().deserialize("test", bytes);

        assertEquals(2, decoded.getCount());
        assertEquals(18, decoded.getTotalDurationHours());
        assertEquals(9.0, decoded.averageDurationHours());
        assertEquals(-1, json.indexOf("averageDurationHours"));
    }
}
