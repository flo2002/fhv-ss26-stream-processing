package fhv.streamprocessing.pattern9.wetdry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import fhv.streamprocessing.model.NoaaObservation;
import fhv.streamprocessing.serde.JsonSerde;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
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

class WetDryPeriodTopologyTest {
    @Test
    void emitsWetPeriodWhenDryObservationClosesOpenWetState() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(WetDryPeriodTopology.stateStore());
        WetDryPeriodTopology.build(
            builder.stream("observations", Consumed.with(Serdes.String(), new JsonSerde<>(NoaaObservation.class))),
            2025
        ).to("wet-periods", Produced.with(Serdes.String(), new JsonSerde<>(WetPeriodEvent.class)));

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), properties())) {
            TestInputTopic<String, NoaaObservation> input = driver.createInputTopic(
                "observations",
                Serdes.String().serializer(),
                new JsonSerde<>(NoaaObservation.class).serializer()
            );
            TestOutputTopic<String, WetPeriodEvent> output = driver.createOutputTopic(
                "wet-periods",
                Serdes.String().deserializer(),
                new JsonSerde<>(WetPeriodEvent.class).deserializer()
            );

            input.pipeInput("station-a", observation("station-a", "2025-03-01T01:00:00Z", 1));
            input.pipeInput("station-a", observation("station-a", "2025-03-01T02:00:00Z", 6));
            input.pipeInput("station-a", observation("station-a", "2025-03-01T04:30:00Z", null));

            WetPeriodEvent event = output.readValue();
            assertEquals("station-a", event.stationId());
            assertEquals(Instant.parse("2025-03-01T01:00:00Z"), event.periodStart());
            assertEquals(Instant.parse("2025-03-01T04:30:00Z"), event.periodEnd());
            assertEquals(210, event.durationMinutes());
            assertEquals(2, event.precipitationObservationCount());
            assertEquals(Instant.parse("2025-03-01T04:30:00Z"), event.closingObservedAt());
            assertTrue(output.isEmpty());
        }
    }

    @Test
    void keepsWetPeriodOpenUntilDryObservationArrives() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(WetDryPeriodTopology.stateStore());
        WetDryPeriodTopology.build(
            builder.stream("observations", Consumed.with(Serdes.String(), new JsonSerde<>(NoaaObservation.class))),
            2025
        ).to("wet-periods", Produced.with(Serdes.String(), new JsonSerde<>(WetPeriodEvent.class)));

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), properties())) {
            TestInputTopic<String, NoaaObservation> input = driver.createInputTopic(
                "observations",
                Serdes.String().serializer(),
                new JsonSerde<>(NoaaObservation.class).serializer()
            );
            TestOutputTopic<String, WetPeriodEvent> output = driver.createOutputTopic(
                "wet-periods",
                Serdes.String().deserializer(),
                new JsonSerde<>(WetPeriodEvent.class).deserializer()
            );

            input.pipeInput("station-a", observation("station-a", "2025-03-01T01:00:00Z", 1));
            input.pipeInput("station-a", observation("station-a", "2025-03-01T02:00:00Z", 6));

            assertTrue(output.isEmpty());
        }
    }

    private static NoaaObservation observation(String stationId, String observedAt, Integer rainDurationHours) {
        OffsetDateTime timestamp = OffsetDateTime.parse(observedAt);
        return new NoaaObservation(
            stationId,
            LocalDate.from(timestamp),
            timestamp,
            8.0,
            "1",
            2.0,
            "1",
            80.0,
            "1",
            rainDurationHours,
            "test",
            1,
            ""
        );
    }

    private static Properties properties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wet-dry-period-test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        return properties;
    }
}
