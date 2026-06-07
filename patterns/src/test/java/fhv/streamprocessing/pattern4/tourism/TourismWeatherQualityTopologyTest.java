package fhv.streamprocessing.pattern4.tourism;

import static org.junit.jupiter.api.Assertions.assertEquals;

import fhv.streamprocessing.model.NoaaObservation;
import fhv.streamprocessing.serde.JsonSerde;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.Test;

class TourismWeatherQualityTopologyTest {
    @Test
    void scoresRegionWeatherQualityFromTemperatureAndWindInTumblingWindow() {
        StreamsBuilder builder = new StreamsBuilder();
        TourismWeatherQualityTopology.build(
            builder.stream("observations", Consumed.with(Serdes.String(), new JsonSerde<>(NoaaObservation.class))),
            2025,
            24,
            10,
            StationRegionResolver.fromMap(Map.of(
                "010010-99999", new WeatherRegion("NO", "Norway"),
                "010020-99999", new WeatherRegion("NO", "Norway"),
                "722950-23174", new WeatherRegion("US-CA", "United States / CA")
            ))
        ).to("tourism-weather-quality", Produced.with(Serdes.String(), new JsonSerde<>(TourismWeatherQualityEvent.class)));

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), properties())) {
            TestInputTopic<String, NoaaObservation> input = driver.createInputTopic(
                "observations",
                Serdes.String().serializer(),
                new JsonSerde<>(NoaaObservation.class).serializer()
            );
            TestOutputTopic<String, TourismWeatherQualityEvent> output = driver.createOutputTopic(
                "tourism-weather-quality",
                Serdes.String().deserializer(),
                new JsonSerde<>(TourismWeatherQualityEvent.class).deserializer()
            );

            input.pipeInput("010010-99999", observation("010010-99999", "2025-07-01T10:00:00", 22.0, 2.0, 100.0));
            input.pipeInput("010020-99999", observation("010020-99999", "2025-07-01T11:00:00", 24.0, 4.0, 80.0));
            input.pipeInput("722950-23174", observation("722950-23174", "2025-07-01T11:30:00", 30.0, 2.0, 100.0));

            TourismWeatherQualityEvent first = output.readKeyValue().value;
            TourismWeatherQualityEvent second = output.readKeyValue().value;
            TourismWeatherQualityEvent third = output.readKeyValue().value;

            assertEquals("NO", second.regionId());
            assertEquals("Norway", second.regionName());
            assertEquals(2, second.observationCount());
            assertEquals(23.0, second.avgTemperatureCelsius());
            assertEquals(3.0, second.avgWindSpeedMetersPerSecond());
            assertEquals(90.0, second.skyClarityScore());
            assertEquals(92.1, second.qualityScore());
            assertEquals("excellent", second.qualityClass());
            assertEquals(first.windowStart(), second.windowStart());
            assertEquals("US-CA", third.regionId());
            assertEquals("United States / CA", third.regionName());
            assertEquals(1, third.observationCount());
        }
    }

    @Test
    void skipsObservationsWithRejectedQualityCodes() {
        StreamsBuilder builder = new StreamsBuilder();
        TourismWeatherQualityTopology.build(
            builder.stream("observations", Consumed.with(Serdes.String(), new JsonSerde<>(NoaaObservation.class))),
            2025,
            24,
            10
        ).to("tourism-weather-quality", Produced.with(Serdes.String(), new JsonSerde<>(TourismWeatherQualityEvent.class)));

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), properties())) {
            TestInputTopic<String, NoaaObservation> input = driver.createInputTopic(
                "observations",
                Serdes.String().serializer(),
                new JsonSerde<>(NoaaObservation.class).serializer()
            );
            TestOutputTopic<String, TourismWeatherQualityEvent> output = driver.createOutputTopic(
                "tourism-weather-quality",
                Serdes.String().deserializer(),
                new JsonSerde<>(TourismWeatherQualityEvent.class).deserializer()
            );

            input.pipeInput("010010-99999", new NoaaObservation(
                "010010-99999",
                LocalDate.of(2025, 7, 1),
                LocalDateTime.parse("2025-07-01T10:00:00").atOffset(ZoneOffset.UTC),
                22.0,
                "9",
                2.0,
                "1",
                100.0,
                "1",
                null,
                null,
                1,
                null
            ));

            assertEquals(0, output.getQueueSize());
        }
    }

    private static Properties properties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "tourism-weather-quality-test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        return properties;
    }

    private static NoaaObservation observation(String stationId, String observedAt, double temperatureCelsius, double windMps, double skyClarityScore) {
        var timestamp = LocalDateTime.parse(observedAt).atOffset(ZoneOffset.UTC);
        return new NoaaObservation(
            stationId,
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
