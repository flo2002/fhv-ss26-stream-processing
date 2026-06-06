package fhv.streamprocessing.pattern6.temperatureranking;

import fhv.streamprocessing.model.NoaaObservation;
import fhv.streamprocessing.serde.JsonSerde;
import java.time.LocalDate;
import java.time.ZoneOffset;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

public final class AnnualPeakTemperatureRankingTopology {
    private AnnualPeakTemperatureRankingTopology() {
    }

    public static KTable<String, TemperatureRankingAggregate> build(KStream<String, NoaaObservation> observations, int rankingYear) {
        RankingWindowKey rankingWindow = rankingWindow(rankingYear);

        return observations
            .filter((key, observation) -> isUsableObservation(observation))
            .filter((key, observation) -> observation.observedAt() != null)
            .filter((key, observation) -> observation.observedAt().getYear() == rankingYear)
            .selectKey((key, observation) -> observation.stationId())
            .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(NoaaObservation.class)))
            .aggregate(
                TemperatureWindowStats::new,
                (stationId, observation, aggregate) -> aggregate.add(observation),
                Materialized.with(Serdes.String(), new JsonSerde<>(TemperatureWindowStats.class))
            )
            .toStream()
            .map((stationId, stats) -> KeyValue.pair(
                rankingWindow.asKey(),
                new StationWindowTemperatureStats(
                    stationId,
                    rankingWindow.windowStartEpochMs(),
                    rankingWindow.windowEndEpochMs(),
                    stats.getCount(),
                    stats.getMinTemperatureCelsius(),
                    stats.getMaxTemperatureCelsius(),
                    stats.averageTemperatureCelsius()
                )
            ))
            .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(StationWindowTemperatureStats.class)))
            .aggregate(
                TemperatureRankingAggregate::new,
                (windowKey, stationStats, aggregate) -> aggregate.update(stationStats),
                Materialized.with(Serdes.String(), new JsonSerde<>(TemperatureRankingAggregate.class))
            );
    }

    public static RankingWindowKey rankingWindow(int rankingYear) {
        long windowStartEpochMs = LocalDate.of(rankingYear, 1, 1)
            .atStartOfDay()
            .toInstant(ZoneOffset.UTC)
            .toEpochMilli();
        long windowEndEpochMs = LocalDate.of(rankingYear + 1, 1, 1)
            .atStartOfDay()
            .toInstant(ZoneOffset.UTC)
            .toEpochMilli();
        return new RankingWindowKey(windowStartEpochMs, windowEndEpochMs);
    }

    private static boolean isUsableObservation(NoaaObservation observation) {
        return observation.stationId() != null
            && observation.observationDate() != null
            && observation.temperatureCelsius() != null
            && observation.temperatureQualityCode() != null
            && !observation.temperatureQualityCode().equals("9");
    }
}
