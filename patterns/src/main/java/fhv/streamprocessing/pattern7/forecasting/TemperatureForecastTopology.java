package fhv.streamprocessing.pattern7.forecasting;

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
 * Kafka implementation: maintains least-squares regression state in
 * overlapping event-time windows and emits a 24-hour projection.
 */
public final class TemperatureForecastTopology {
    private TemperatureForecastTopology() {
    }

    public static KStream<String, TemperatureForecastEvent> build(
        KStream<String, NoaaObservation> observations,
        int windowDays,
        int advanceDays,
        int graceMinutes
    ) {
        // advanceBy makes this a sliding/hopping window instead of a tumbling window.
        TimeWindows window = TimeWindows.ofSizeAndGrace(Duration.ofDays(windowDays), Duration.ofMinutes(graceMinutes))
            .advanceBy(Duration.ofDays(advanceDays));

        return observations
            .filter((key, observation) -> observation.stationId() != null && observation.observedAt() != null)
            .filter((key, observation) -> observation.temperatureCelsius() != null)
            .selectKey((key, observation) -> observation.stationId())
            .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(NoaaObservation.class)))
            .windowedBy(window)
            // The aggregate stores the sums needed for online least-squares regression.
            .aggregate(
                TemperatureTrendAggregate::new,
                (stationId, observation, aggregate) -> aggregate.add(observation),
                Materialized.with(Serdes.String(), new JsonSerde<>(TemperatureTrendAggregate.class))
            )
            .toStream()
            .map((windowedStationId, aggregate) -> {
                StationForecastKey eventKey = new StationForecastKey(
                    windowedStationId.key(),
                    Instant.ofEpochMilli(windowedStationId.window().start()),
                    Instant.ofEpochMilli(windowedStationId.window().end())
                );

                // Slope is degrees per second because observation timestamps are epoch seconds.
                double slope = aggregate.getSlope();
                double currentAverage = aggregate.getLatestAverage();
                // Anchor the forecast at the fitted value of the latest observation ("now"),
                // then project the trend forward 24h, instead of anchoring at the window mean.
                double valueAtLastObservation = aggregate.getValueAtLastObservation();
                double forecastNext24h = valueAtLastObservation + (slope * 86400);

                TemperatureForecastEvent event = new TemperatureForecastEvent(
                    windowedStationId.key(),
                    Instant.ofEpochMilli(windowedStationId.window().start()),
                    Instant.ofEpochMilli(windowedStationId.window().end()),
                    slope,
                    currentAverage,
                    forecastNext24h,
                    aggregate.getCount()
                );
                return KeyValue.pair(eventKey.asKey(), event);
            });
    }
}
