package fhv.streamprocessing.pattern5.rainduration;

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
 * Kafka implementation: groups rainy observations by station-year and keeps
 * a materialized, recoverable running duration aggregate.
 */
public final class YearlyRainDurationTopology {
    private YearlyRainDurationTopology() {
    }

    public static KTable<String, RainDurationAggregate> build(KStream<String, NoaaObservation> observations, int year) {
        // Station-year is the business key: all rainy observations for that result share state.
        KGroupedStream<String, NoaaObservation> groupedByStationYear = observations
            // Zero/missing durations do not represent a measured rain period.
            .filter((key, observation) -> isUsableObservation(observation))
            .filter((key, observation) -> observation.observationDate().getYear() == year)
            .map((key, observation) -> KeyValue.pair(StationYearKey.fromObservation(observation).asKey(), observation))
            .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(NoaaObservation.class)));

        // The initializer creates empty state for a new station-year; add()
        // updates total and count. Kafka backs the materialized result by a changelog.
        return groupedByStationYear.aggregate(
            RainDurationAggregate::new,
            (stationYear, observation, aggregate) -> aggregate.add(observation.rainDurationHours()),
            Materialized.with(Serdes.String(), new JsonSerde<>(RainDurationAggregate.class))
        );
    }

    public static boolean isUsableObservation(NoaaObservation observation) {
        return observation.stationId() != null
            && observation.observationDate() != null
            && observation.rainDurationHours() != null
            && observation.rainDurationHours() > 0;
    }
}
