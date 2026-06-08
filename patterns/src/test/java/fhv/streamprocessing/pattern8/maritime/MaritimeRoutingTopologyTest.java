package fhv.streamprocessing.pattern8.maritime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import fhv.streamprocessing.serde.JsonSerde;
import java.time.Instant;
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

class MaritimeRoutingTopologyTest {
    @Test
    void joinsAisAndBuoyEventsBySeaAreaWithinWindow() {
        StreamsBuilder builder = new StreamsBuilder();
        MaritimeRoutingTopology.build(
            builder.stream("ais", Consumed.with(Serdes.String(), new JsonSerde<>(AisPositionEvent.class))),
            builder.stream("buoys", Consumed.with(Serdes.String(), new JsonSerde<>(BuoyObservationEvent.class))),
            2025,
            30
        ).to("recommendations", Produced.with(Serdes.String(), new JsonSerde<>(RouteRecommendationEvent.class)));

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), properties())) {
            TestInputTopic<String, BuoyObservationEvent> buoyTopic = driver.createInputTopic(
                "buoys",
                Serdes.String().serializer(),
                new JsonSerde<>(BuoyObservationEvent.class).serializer()
            );
            TestInputTopic<String, AisPositionEvent> aisTopic = driver.createInputTopic(
                "ais",
                Serdes.String().serializer(),
                new JsonSerde<>(AisPositionEvent.class).serializer()
            );
            TestOutputTopic<String, RouteRecommendationEvent> outputTopic = driver.createOutputTopic(
                "recommendations",
                Serdes.String().deserializer(),
                new JsonSerde<>(RouteRecommendationEvent.class).deserializer()
            );

            buoyTopic.pipeInput("CARICOOS-RINCON", buoy("CARIB_PR_WEST", Instant.parse("2025-06-01T10:00:00Z"), 3.2, 16.0));
            aisTopic.pipeInput("366999001", ais("366999001", "CARIBBEAN TRADER", "CARIB_PR_WEST", Instant.parse("2025-06-01T10:10:00Z")));

            RouteRecommendationEvent event = outputTopic.readValue();
            assertEquals("366999001", event.mmsi());
            assertEquals("CARIBBEAN TRADER", event.vesselName());
            assertEquals("CARIB_PR_WEST", event.seaAreaId());
            assertEquals("HIGH", event.riskClass());
            assertEquals(100.0, event.riskScore());
            assertEquals(90, event.etaDelayMinutes());
            assertEquals(Instant.parse("2025-06-01T17:30:00Z"), event.updatedEta());
            assertEquals(18.42, event.latitude());
            assertEquals(-67.15, event.longitude());
            assertTrue(outputTopic.isEmpty());
        }
    }

    @Test
    void ignoresEventsOutsideConfiguredYear() {
        StreamsBuilder builder = new StreamsBuilder();
        MaritimeRoutingTopology.build(
            builder.stream("ais", Consumed.with(Serdes.String(), new JsonSerde<>(AisPositionEvent.class))),
            builder.stream("buoys", Consumed.with(Serdes.String(), new JsonSerde<>(BuoyObservationEvent.class))),
            2025,
            30
        ).to("recommendations", Produced.with(Serdes.String(), new JsonSerde<>(RouteRecommendationEvent.class)));

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), properties())) {
            TestInputTopic<String, BuoyObservationEvent> buoyTopic = driver.createInputTopic(
                "buoys",
                Serdes.String().serializer(),
                new JsonSerde<>(BuoyObservationEvent.class).serializer()
            );
            TestInputTopic<String, AisPositionEvent> aisTopic = driver.createInputTopic(
                "ais",
                Serdes.String().serializer(),
                new JsonSerde<>(AisPositionEvent.class).serializer()
            );
            TestOutputTopic<String, RouteRecommendationEvent> outputTopic = driver.createOutputTopic(
                "recommendations",
                Serdes.String().deserializer(),
                new JsonSerde<>(RouteRecommendationEvent.class).deserializer()
            );

            buoyTopic.pipeInput("CARICOOS-RINCON", buoy("CARIB_PR_WEST", Instant.parse("2024-06-01T10:00:00Z"), 3.2, 16.0));
            aisTopic.pipeInput("366999001", ais("366999001", "CARIBBEAN TRADER", "CARIB_PR_WEST", Instant.parse("2024-06-01T10:10:00Z")));

            assertTrue(outputTopic.isEmpty());
        }
    }

    private static BuoyObservationEvent buoy(String seaAreaId, Instant observedAt, double waveHeightMeters, double windMps) {
        return new BuoyObservationEvent(
            "CARICOOS-RINCON",
            observedAt,
            18.36,
            -67.25,
            seaAreaId,
            waveHeightMeters,
            windMps,
            80.0
        );
    }

    private static AisPositionEvent ais(String mmsi, String vesselName, String seaAreaId, Instant observedAt) {
        return new AisPositionEvent(
            mmsi,
            vesselName,
            observedAt,
            18.42,
            -67.15,
            12.4,
            102.0,
            "SAN JUAN",
            Instant.parse("2025-06-01T16:00:00Z"),
            seaAreaId
        );
    }

    private static Properties properties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "maritime-routing-test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        return properties;
    }
}
