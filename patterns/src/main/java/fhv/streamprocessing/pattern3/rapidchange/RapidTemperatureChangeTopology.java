package fhv.streamprocessing.pattern3.rapidchange;

import fhv.streamprocessing.model.NoaaObservation;
import fhv.streamprocessing.serde.JsonSerde;
import java.time.Duration;
import java.time.Instant;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;

/**
 * Kafka implementation: partitions by station and aggregates temperature
 * change rates inside non-overlapping event-time windows.
 */
public final class RapidTemperatureChangeTopology {
    private RapidTemperatureChangeTopology() {
    }

    public static KStream<String, RapidTemperatureChangeEvent> build(
        KStream<String, NoaaObservation> observations,
        int detectionYear,
        int windowHours,
        int graceMinutes
    ) {
        // No advanceBy call means adjacent, non-overlapping (tumbling) event-time windows.
        TimeWindows tumblingWindow = TimeWindows.ofSizeAndGrace(Duration.ofHours(windowHours), Duration.ofMinutes(graceMinutes));

        return observations
            // Validate and re-key before grouping so each station is evaluated independently.
            .filter((key, observation) -> observation.stationId() != null && observation.observedAt() != null)
            .filter((key, observation) -> observation.observedAt().getYear() == detectionYear)
            .filter((key, observation) -> observation.temperatureCelsius() != null)
            .selectKey((key, observation) -> observation.stationId())
            .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(NoaaObservation.class)))
            .windowedBy(tumblingWindow)
            // The aggregate keeps consecutive readings and min/max/average change rates.
            .aggregate(
                RapidTemperatureChangeAggregate::new,
                (stationId, observation, aggregate) -> aggregate.add(observation),
                Materialized.with(Serdes.String(), new JsonSerde<>(RapidTemperatureChangeAggregate.class))
            )
            .toStream()
            // Convert Kafka's Windowed key into the stable event key used downstream.
            .map((windowedStationId, aggregate) -> {
                StationRapidChangeWindowKey eventKey = new StationRapidChangeWindowKey(
                    windowedStationId.key(),
                    windowedStationId.window().start(),
                    windowedStationId.window().end()
                );
                RapidTemperatureChangeEvent event = new RapidTemperatureChangeEvent(
                    windowedStationId.key(),
                    Instant.ofEpochMilli(windowedStationId.window().start()),
                    Instant.ofEpochMilli(windowedStationId.window().end()),
                    aggregate.getMinRateOfChange(),
                    aggregate.getMaxRateOfChange(),
                    aggregate.getAverageRateOfChange(),
                    aggregate.getRateOfChangeCount()
                );
                return KeyValue.pair(eventKey.asKey(), event);
            });
    }
}
