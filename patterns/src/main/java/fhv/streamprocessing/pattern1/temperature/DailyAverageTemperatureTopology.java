package fhv.streamprocessing.pattern1.temperature;

import fhv.streamprocessing.model.NoaaObservation;
import fhv.streamprocessing.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

/**
 * Kafka implementation: re-keys observations by station-day and materializes
 * a changelog-backed KTable containing the continuously updated average.
 */
public final class DailyAverageTemperatureTopology {
    private DailyAverageTemperatureTopology() {
    }

    public static KTable<String, TemperatureAggregate> build(KStream<String, NoaaObservation> observations) {
        // Re-key by station and calendar day so every update for one daily result
        // reaches the same Kafka Streams partition and state-store entry.
        KGroupedStream<String, NoaaObservation> groupedByStationDay = observations
            // Bad quality code "9" and missing values must not affect the average.
            .filter((key, observation) -> isUsableObservation(observation))
            .map((key, observation) -> KeyValue.pair(StationDayKey.fromObservation(observation).asKey(), observation))
            .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(NoaaObservation.class)));

        // Kafka calls the initializer once per new key and the aggregator once
        // per record. Materialization persists the result and its changelog.
        return groupedByStationDay.aggregate(
            TemperatureAggregate::new,
            (stationDay, observation, aggregate) -> aggregate.add(observation.temperatureCelsius()),
            Materialized.with(Serdes.String(), new JsonSerde<>(TemperatureAggregate.class))
        );
    }

    public static boolean isUsableObservation(NoaaObservation observation) {
        return observation.stationId() != null
            && observation.observationDate() != null
            && observation.temperatureCelsius() != null
            && observation.temperatureQualityCode() != null
            && !observation.temperatureQualityCode().equals("9");
    }
}
