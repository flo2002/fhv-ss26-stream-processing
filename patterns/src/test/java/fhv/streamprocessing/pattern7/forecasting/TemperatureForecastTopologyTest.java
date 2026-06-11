package fhv.streamprocessing.pattern7.forecasting;

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
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.junit.jupiter.api.Test;

class TemperatureForecastTopologyTest {
    @Test
    void forecastCalculatesTrendAndPredictsFutureTemperature() {
        StreamsBuilder builder = new StreamsBuilder();
        TemperatureForecastTopology.build(
            builder.stream("observations", Consumed.with(org.apache.kafka.common.serialization.Serdes.String(), new JsonSerde<>(NoaaObservation.class))),
            30,
            1,
            10
        )
            .to("temperature-forecast", org.apache.kafka.streams.kstream.Produced.with(org.apache.kafka.common.serialization.Serdes.String(), new JsonSerde<>(TemperatureForecastEvent.class)));

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "temperature-forecast-test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), properties)) {
            TestInputTopic<String, NoaaObservation> inputTopic = driver.createInputTopic(
                "observations",
                new StringSerializer(),
                new JsonSerde<>(NoaaObservation.class).serializer()
            );
            TestOutputTopic<String, TemperatureForecastEvent> outputTopic = driver.createOutputTopic(
                "temperature-forecast",
                new StringDeserializer(),
                new JsonSerde<>(TemperatureForecastEvent.class).deserializer()
            );

            // Linear increase: 1 degree per hour
            // 2025-01-01T00:00:00Z -> 10.0
            // 2025-01-01T01:00:00Z -> 11.0
            // 2025-01-01T02:00:00Z -> 12.0
            inputTopic.pipeInput("010010-99999", observation("010010-99999", LocalDateTime.of(2025, 1, 1, 0, 0), 10.0), Instant.parse("2025-01-01T00:00:00Z"));
            inputTopic.pipeInput("010010-99999", observation("010010-99999", LocalDateTime.of(2025, 1, 1, 1, 0), 11.0), Instant.parse("2025-01-01T01:00:00Z"));
            inputTopic.pipeInput("010010-99999", observation("010010-99999", LocalDateTime.of(2025, 1, 1, 2, 0), 12.0), Instant.parse("2025-01-01T02:00:00Z"));

            var results = outputTopic.readKeyValuesToMap();
            // The window is 30 days starting at 2025-01-01T00:00:00Z (epoch 0 relative to days)
            // Wait, Kafka Streams windows start at epoch 0.
            // 2025-01-01 is epoch day 20089.
            // Window start will be 20089 * 86400 * 1000 or similar.
            
            assertTrue(!results.isEmpty());
            
            // Get the first (and only) result for this station
            TemperatureForecastEvent event = results.values().iterator().next();
            
            assertEquals("010010-99999", event.stationId());
            // Slope should be 1 degree per hour = 1/3600 per second
            assertEquals(1.0 / 3600.0, event.slopePerSecond(), 0.000001);
            assertEquals(11.0, event.currentAverage(), 0.000001);
            // Linear regression with points (0, 10), (3600, 11), (7200, 12)
            // x_mean = 3600, y_mean = 11
            // sum((x-x_mean)(y-y_mean)) = (-3600*-1) + (0*0) + (3600*1) = 7200
            // sum((x-x_mean)^2) = (-3600)^2 + 0^2 + 3600^2 = 2 * 3600^2
            // slope = 7200 / (2 * 3600^2) = 1 / 3600  (i.e. 1 C/h)
            // Forecast is anchored at the latest observation, not the window mean:
            // fitted value at the latest reading (t=7200) = 12.0, then +24h of trend:
            // 12 + (1/3600 * 86400) = 12 + 24 = 36
            assertEquals(36.0, event.forecastNext24h(), 0.000001);
        }
    }

    private static NoaaObservation observation(String stationId, LocalDateTime observedAt, double temperatureCelsius) {
        OffsetDateTime offsetDateTime = observedAt.atOffset(ZoneOffset.UTC);
        return new NoaaObservation(
            stationId,
            observedAt.toLocalDate(),
            offsetDateTime,
            temperatureCelsius,
            "1",
            null,
            null,
            null,
            null,
            null,
            "/tmp/source",
            1L,
            "raw"
        );
    }
}
