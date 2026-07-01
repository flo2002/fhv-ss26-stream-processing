package fhv.streamprocessing.pattern11.weatherregime;

import fhv.streamprocessing.model.NoaaObservation;
import fhv.streamprocessing.pattern1.temperature.StationDayKey;
import fhv.streamprocessing.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;

/**
 * Kafka implementation: aggregates station-day feature vectors and feeds each
 * completed vector into the incremental CluStream model.
 */
public final class WeatherRegimeTopology {
    private WeatherRegimeTopology() {
    }

    public static KStream<String, WeatherRegimeEvent> build(
        KStream<String, NoaaObservation> observations,
        int weatherRegimeYear,
        int clusterCount,
        int horizon
    ) {
        // One online learner is updated incrementally as completed station-day features arrive.
        OnlineWeatherRegimeClusterer clusterer = new OnlineWeatherRegimeClusterer(clusterCount, horizon);
        return observations
            // Accept partial observations when at least one feature is usable.
            .filter((key, observation) -> isUsableObservation(observation, weatherRegimeYear))
            .map((key, observation) -> KeyValue.pair(StationDayKey.fromObservation(observation).asKey(), observation))
            .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(NoaaObservation.class)))
            // Collapse raw observations into one multi-feature vector per station and day.
            .aggregate(
                StationDayWeatherAggregate::new,
                (stationDay, observation, aggregate) -> aggregate.add(observation),
                Materialized.with(Serdes.String(), new JsonSerde<>(StationDayWeatherAggregate.class))
            )
            .toStream()
            // Do not train CluStream on an empty/insufficient daily feature vector.
            .filter((stationDay, aggregate) -> aggregate.hasEnoughSignal())
            // Assign the current vector, then train CluStream for the following records.
            .mapValues((stationDay, aggregate) -> {
                StationDayKey key = StationDayKey.parse(stationDay);
                WeatherRegimeAssignment assignment = clusterer.assignAndLearn(aggregate.toFeatureVector());
                return new WeatherRegimeEvent(
                    key.stationId(),
                    key.day(),
                    assignment.clusterId(),
                    assignment.clusterLabel(),
                    assignment.distanceToCenter(),
                    aggregate.avgTemperatureCelsius(),
                    aggregate.minTemperatureCelsius(),
                    aggregate.maxTemperatureCelsius(),
                    aggregate.temperatureRangeCelsius(),
                    aggregate.avgWindSpeedMetersPerSecond(),
                    aggregate.maxWindSpeedMetersPerSecond(),
                    aggregate.skyClarityScore(),
                    aggregate.precipitationObservationCount(),
                    aggregate.rainDurationHours(),
                    aggregate.observationCount()
                );
            });
    }

    private static boolean isUsableObservation(NoaaObservation observation, int weatherRegimeYear) {
        return observation.stationId() != null
            && observation.observationDate() != null
            && observation.observationDate().getYear() == weatherRegimeYear
            && (
                hasUsableTemperature(observation)
                    || hasUsableWindSpeed(observation)
                    || hasUsableSkyClarity(observation)
                    || observation.rainDurationHours() != null
            );
    }

    private static boolean hasUsableTemperature(NoaaObservation observation) {
        return observation.temperatureCelsius() != null
            && observation.temperatureQualityCode() != null
            && !observation.temperatureQualityCode().equals("9");
    }

    private static boolean hasUsableWindSpeed(NoaaObservation observation) {
        return observation.windSpeedMetersPerSecond() != null
            && observation.windSpeedQualityCode() != null
            && !observation.windSpeedQualityCode().equals("9");
    }

    private static boolean hasUsableSkyClarity(NoaaObservation observation) {
        return observation.skyClarityScore() != null
            && observation.visibilityQualityCode() != null
            && !observation.visibilityQualityCode().equals("9");
    }
}
