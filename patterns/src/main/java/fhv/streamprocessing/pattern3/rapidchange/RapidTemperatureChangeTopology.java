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

public final class RapidTemperatureChangeTopology {
    private RapidTemperatureChangeTopology() {
    }

    public static KStream<String, RapidTemperatureChangeEvent> build(
        KStream<String, NoaaObservation> observations,
        int detectionYear,
        int windowHours,
        int graceMinutes
    ) {
        TimeWindows tumblingWindow = TimeWindows.ofSizeAndGrace(Duration.ofHours(windowHours), Duration.ofMinutes(graceMinutes));

        return observations
            .filter((key, observation) -> observation.stationId() != null && observation.observedAt() != null)
            .filter((key, observation) -> observation.observedAt().getYear() == detectionYear)
            .filter((key, observation) -> observation.temperatureCelsius() != null)
            .selectKey((key, observation) -> observation.stationId())
            .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(NoaaObservation.class)))
            .windowedBy(tumblingWindow)
            .aggregate(
                RapidTemperatureChangeAggregate::new,
                (stationId, observation, aggregate) -> aggregate.add(observation),
                Materialized.with(Serdes.String(), new JsonSerde<>(RapidTemperatureChangeAggregate.class))
            )
            .toStream()
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
