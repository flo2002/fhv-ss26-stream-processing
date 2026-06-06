package fhv.streamprocessing.pattern10.blizzard;

import fhv.streamprocessing.model.NoaaObservation;
import fhv.streamprocessing.serde.JsonSerde;
import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;

public final class BlizzardDetectionTopology {
    public static final String EVENT_TYPE = "blizzard-like-conditions";

    private BlizzardDetectionTopology() {
    }

    public static KStream<String, BlizzardEvent> build(
        KStream<String, NoaaObservation> observations,
        int detectionYear,
        double freezingThresholdCelsius,
        double strongWindThresholdMetersPerSecond,
        int windowHours,
        int advanceHours,
        int graceMinutes
    ) {
        TimeWindows detectionWindow = TimeWindows.ofSizeAndGrace(Duration.ofHours(windowHours), Duration.ofMinutes(graceMinutes))
            .advanceBy(Duration.ofHours(advanceHours));

        return observations
            .filter((key, observation) -> observation.stationId() != null && observation.observedAt() != null)
            .filter((key, observation) -> observation.observedAt().getYear() == detectionYear)
            .filter((key, observation) -> hasAnyRelevantSignal(observation))
            .selectKey((key, observation) -> observation.stationId())
            .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(NoaaObservation.class)))
            .windowedBy(detectionWindow)
            .aggregate(
                BlizzardWindowAggregate::new,
                (stationId, observation, aggregate) -> aggregate.add(
                    observation,
                    freezingThresholdCelsius,
                    strongWindThresholdMetersPerSecond
                ),
                Materialized.with(Serdes.String(), new JsonSerde<>(BlizzardWindowAggregate.class))
            )
            .toStream()
            .map((windowedStationId, aggregate) -> {
                StationBlizzardWindowKey eventKey = new StationBlizzardWindowKey(
                    windowedStationId.key(),
                    windowedStationId.window().start(),
                    windowedStationId.window().end()
                );
                BlizzardEvent event = new BlizzardEvent(
                    windowedStationId.key(),
                    windowedStationId.window().start(),
                    windowedStationId.window().end(),
                    EVENT_TYPE,
                    freezingThresholdCelsius,
                    strongWindThresholdMetersPerSecond,
                    aggregate.getSampleCount(),
                    aggregate.getFreezingObservationCount(),
                    aggregate.getStrongWindObservationCount(),
                    aggregate.getPrecipitationObservationCount(),
                    aggregate.getMinTemperatureCelsius(),
                    aggregate.getMaxWindSpeedMetersPerSecond()
                );
                return KeyValue.pair(eventKey.asKey(), event);
            })
            .filter((eventKey, event) -> event.isDetected());
    }

    private static boolean hasAnyRelevantSignal(NoaaObservation observation) {
        return hasUsableTemperature(observation)
            || hasUsableWindSpeed(observation)
            || hasMeasuredPrecipitation(observation);
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

    private static boolean hasMeasuredPrecipitation(NoaaObservation observation) {
        return observation.rainDurationHours() != null && observation.rainDurationHours() > 0;
    }
}
