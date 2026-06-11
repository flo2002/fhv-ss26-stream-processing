package fhv.streamprocessing.pattern3.rapidchange;

import static org.junit.jupiter.api.Assertions.assertEquals;

import fhv.streamprocessing.model.NoaaObservation;
import fhv.streamprocessing.serde.JsonSerde;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.Test;

class RapidTemperatureChangeTopologyTest {

    @Test
    void monotonicWarmingReportsSmallestAndLargestPositiveRate() {
        // Rates: 10->12 over 1h = +2 C/h, 12->15 over 1h = +3 C/h (both positive).
        // This guards the serialization regression where min was clamped to 0.0.
        var event = runSingleWindow(
            obs(LocalDateTime.of(2025, 1, 1, 0, 0), 10.0),
            obs(LocalDateTime.of(2025, 1, 1, 1, 0), 12.0),
            obs(LocalDateTime.of(2025, 1, 1, 2, 0), 15.0)
        );

        assertEquals(2, event.rateOfChangeCount());
        assertEquals(2.0, event.minRateOfChange(), 1e-9);
        assertEquals(3.0, event.maxRateOfChange(), 1e-9);
        assertEquals(2.5, event.averageRateOfChange(), 1e-9);
    }

    @Test
    void mixedWarmingAndCoolingReportsSignedExtremes() {
        // Rates: 10->14 over 1h = +4 C/h, 14->9 over 1h = -5 C/h.
        var event = runSingleWindow(
            obs(LocalDateTime.of(2025, 1, 1, 0, 0), 10.0),
            obs(LocalDateTime.of(2025, 1, 1, 1, 0), 14.0),
            obs(LocalDateTime.of(2025, 1, 1, 2, 0), 9.0)
        );

        assertEquals(2, event.rateOfChangeCount());
        assertEquals(-5.0, event.minRateOfChange(), 1e-9);
        assertEquals(4.0, event.maxRateOfChange(), 1e-9);
        assertEquals(-0.5, event.averageRateOfChange(), 1e-9);
    }

    private static RapidTemperatureChangeEvent runSingleWindow(NoaaObservation... observations) {
        StreamsBuilder builder = new StreamsBuilder();
        RapidTemperatureChangeTopology.build(
            builder.stream("obs", Consumed.with(Serdes.String(), new JsonSerde<>(NoaaObservation.class))),
            2025, 24, 10
        ).to("out", Produced.with(Serdes.String(), new JsonSerde<>(RapidTemperatureChangeEvent.class)));

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "rapid-change-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            TestInputTopic<String, NoaaObservation> in = driver.createInputTopic(
                "obs", new StringSerializer(), new JsonSerde<>(NoaaObservation.class).serializer());
            TestOutputTopic<String, RapidTemperatureChangeEvent> out = driver.createOutputTopic(
                "out", new StringDeserializer(), new JsonSerde<>(RapidTemperatureChangeEvent.class).deserializer());

            for (NoaaObservation observation : observations) {
                in.pipeInput("S", observation, observation.observedAt().toInstant());
            }

            // All observations fall in the same 24h tumbling window; the last emitted
            // update carries the fully aggregated min/max/avg for that window.
            return out.readKeyValuesToMap().values().stream().reduce((a, b) -> b).orElseThrow();
        }
    }

    private static NoaaObservation obs(LocalDateTime at, double tempC) {
        OffsetDateTime odt = at.atOffset(ZoneOffset.UTC);
        return new NoaaObservation("S", at.toLocalDate(), odt, tempC, "1",
            null, null, null, null, null, "/tmp", 1L, "raw");
    }
}
