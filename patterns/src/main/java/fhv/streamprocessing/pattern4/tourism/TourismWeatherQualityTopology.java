package fhv.streamprocessing.pattern4.tourism;

import fhv.streamprocessing.model.NoaaObservation;
import fhv.streamprocessing.serde.JsonSerde;
import java.time.Duration;
import java.time.Instant;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;

/**
 * Kafka implementation: enriches station records with a region key and
 * aggregates the scoring inputs in regional event-time windows.
 */
public final class TourismWeatherQualityTopology {
    private TourismWeatherQualityTopology() {
    }

    public static KStream<String, TourismWeatherQualityEvent> build(
        KStream<String, NoaaObservation> observations,
        int qualityYear,
        int windowHours,
        int graceMinutes
    ) {
        return build(observations, qualityYear, windowHours, graceMinutes, StationRegionResolver.empty());
    }

    public static KStream<String, TourismWeatherQualityEvent> build(
        KStream<String, NoaaObservation> observations,
        int qualityYear,
        int windowHours,
        int graceMinutes,
        StationRegionResolver regionResolver
    ) {
        // Regional quality is computed in event-time windows, with grace for late records.
        TimeWindows qualityWindow = TimeWindows.ofSizeAndGrace(Duration.ofHours(windowHours), Duration.ofMinutes(graceMinutes));

        return observations
            // Require every signal used by the score so averages share a sample set.
            .filter((key, observation) -> isUsableObservation(observation))
            .filter((key, observation) -> observation.observationDate().getYear() == qualityYear)
            // Translate station IDs to tourism regions before repartitioning and aggregation.
            .map((key, observation) -> KeyValue.pair(regionResolver.resolve(observation.stationId()).id(), observation))
            .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(NoaaObservation.class)))
            // Kafka uses the record timestamp supplied by the TimestampExtractor.
            .windowedBy(qualityWindow)
            .aggregate(
                TourismWeatherQualityAggregate::new,
                addObservation(regionResolver),
                Materialized.with(Serdes.String(), new JsonSerde<>(TourismWeatherQualityAggregate.class))
            )
            .toStream()
            // Publish a readable event instead of exposing Kafka's internal Windowed key.
            .map((windowedRegion, aggregate) -> {
                TourismWeatherQualityEvent event = new TourismWeatherQualityEvent(
                    windowedRegion.key(),
                    aggregate.regionName(),
                    Instant.ofEpochMilli(windowedRegion.window().start()),
                    Instant.ofEpochMilli(windowedRegion.window().end()),
                    aggregate.avgTemperatureCelsius(),
                    aggregate.avgWindSpeedMetersPerSecond(),
                    aggregate.avgSkyClarityScore(),
                    aggregate.qualityScore(),
                    aggregate.qualityClass(),
                    aggregate.observationCount()
                );
                return KeyValue.pair(RegionWindowKey.of(event).asKey(), event);
            });
    }

    private static Aggregator<String, NoaaObservation, TourismWeatherQualityAggregate> addObservation(StationRegionResolver regionResolver) {
        return (regionId, observation, aggregate) -> aggregate.add(observation, regionResolver.resolve(observation.stationId()));
    }

    private static boolean isUsableObservation(NoaaObservation observation) {
        return observation.stationId() != null
            && observation.observationDate() != null
            && observation.temperatureCelsius() != null
            && observation.temperatureQualityCode() != null
            && !observation.temperatureQualityCode().equals("9")
            && observation.windSpeedMetersPerSecond() != null
            && observation.windSpeedQualityCode() != null
            && !observation.windSpeedQualityCode().equals("9")
            && observation.skyClarityScore() != null
            && observation.visibilityQualityCode() != null
            && !observation.visibilityQualityCode().equals("9");
    }
}
