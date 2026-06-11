package fhv.streamprocessing.pattern11.weatherregime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import fhv.streamprocessing.model.NoaaObservation;
import fhv.streamprocessing.serde.JsonSerde;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.Test;

class WeatherRegimeTopologyTest {
    @Test
    void clustersStationDayWeatherFeatureAggregates() {
        StreamsBuilder builder = new StreamsBuilder();
        WeatherRegimeTopology.build(
            builder.stream("observations", Consumed.with(Serdes.String(), new JsonSerde<>(NoaaObservation.class))),
            2025,
            6,
            10_000
        )
            .to("weather-regimes", Produced.with(Serdes.String(), new JsonSerde<>(WeatherRegimeEvent.class)));

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-regime-test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), properties)) {
            TestInputTopic<String, NoaaObservation> inputTopic = driver.createInputTopic(
                "observations",
                new StringSerializer(),
                new JsonSerde<>(NoaaObservation.class).serializer()
            );
            TestOutputTopic<String, WeatherRegimeEvent> outputTopic = driver.createOutputTopic(
                "weather-regimes",
                new StringDeserializer(),
                new JsonSerde<>(WeatherRegimeEvent.class).deserializer()
            );

            inputTopic.pipeInput(
                "record-1",
                observation("010010-99999", LocalDateTime.of(2025, 1, 15, 0, 0), -4.0, 8.0, 70.0, 2),
                Instant.parse("2025-01-15T00:00:00Z")
            );
            inputTopic.pipeInput(
                "record-2",
                observation("010010-99999", LocalDateTime.of(2025, 1, 15, 12, 0), -2.0, 10.0, 80.0, null),
                Instant.parse("2025-01-15T12:00:00Z")
            );
            inputTopic.pipeInput(
                "record-3",
                observation("020020-88888", LocalDateTime.of(2024, 1, 15, 12, 0), 30.0, 2.0, 95.0, null),
                Instant.parse("2024-01-15T12:00:00Z")
            );

            Map<String, WeatherRegimeEvent> latestEvents = new LinkedHashMap<>();
            outputTopic.readKeyValuesToList().forEach(record -> latestEvents.put(record.key, record.value));

            WeatherRegimeEvent event = latestEvents.get("010010-99999|2025-01-15");
            assertEquals("010010-99999", event.stationId());
            assertEquals(-3.0, event.avgTemperatureCelsius());
            assertEquals(9.0, event.avgWindSpeedMetersPerSecond());
            assertEquals(75.0, event.skyClarityScore());
            assertEquals(2L, event.observationCount());
            assertTrue(event.distanceToCenter() >= 0.0);
            assertTrue(!latestEvents.containsKey("020020-88888|2024-01-15"));
        }
    }

    private static NoaaObservation observation(
        String stationId,
        LocalDateTime observedAt,
        Double temperatureCelsius,
        Double windSpeedMetersPerSecond,
        Double skyClarityScore,
        Integer rainDurationHours
    ) {
        OffsetDateTime offsetDateTime = observedAt.atOffset(ZoneOffset.UTC);
        return new NoaaObservation(
            stationId,
            observedAt.toLocalDate(),
            offsetDateTime,
            temperatureCelsius,
            temperatureCelsius == null ? null : "1",
            windSpeedMetersPerSecond,
            windSpeedMetersPerSecond == null ? null : "1",
            skyClarityScore,
            skyClarityScore == null ? null : "1",
            rainDurationHours,
            "/tmp/source",
            1L,
            "raw"
        );
    }
}
