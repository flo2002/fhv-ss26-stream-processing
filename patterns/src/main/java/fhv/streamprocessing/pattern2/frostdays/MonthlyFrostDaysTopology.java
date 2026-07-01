package fhv.streamprocessing.pattern2.frostdays;

import fhv.streamprocessing.model.NoaaObservation;
import fhv.streamprocessing.serde.JsonSerde;
import java.time.LocalDate;
import java.time.YearMonth;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

/**
 * Kafka implementation: collapses frost readings to distinct station-days,
 * then re-groups the resulting KTable rows into monthly counts.
 */
public final class MonthlyFrostDaysTopology {
    private MonthlyFrostDaysTopology() {
    }

    public static KTable<String, Long> build(KStream<String, NoaaObservation> observations, int frostCountYear) {
        // First collapse any number of sub-zero observations into one station-day entry.
        KTable<String, Long> frostObservationsPerStationDay = observations
            .filter((key, observation) -> isUsableObservation(observation))
            .filter((key, observation) -> observation.observationDate().getYear() == frostCountYear)
            .filter((key, observation) -> observation.temperatureCelsius() < 0.0)
            .map((key, observation) -> KeyValue.pair(stationDayKey(observation), observation))
            .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(NoaaObservation.class)))
            .count(Materialized.with(Serdes.String(), Serdes.Long()));

        // Re-group daily entries by month; each existing frost day contributes exactly one.
        KGroupedTable<String, Long> groupedByStationMonth = frostObservationsPerStationDay
            .groupBy(
                (stationDayKey, frostObservations) -> KeyValue.pair(
                    stationMonthKey(stationDayKey),
                    frostObservations > 0 ? 1L : 0L
                ),
                Grouped.with(Serdes.String(), Serdes.Long())
            );

        // A second frost reading changes the daily KTable row from 1 to 2, but
        // both values contribute only one frost day to the month. Kafka therefore
        // removes the old contribution and adds its replacement: total - 1 + 1.
        return groupedByStationMonth.aggregate(
            () -> 0L,
            (stationMonth, newValue, aggregate) -> aggregate + newValue,
            (stationMonth, oldValue, aggregate) -> aggregate - oldValue,
            Materialized.with(Serdes.String(), Serdes.Long())
        );
    }

    public static String stationMonthKey(String stationDayKey) {
        // Convert the first aggregation's station|day key into station|month
        // so Kafka Streams can repartition daily rows for the monthly total.
        String[] parts = stationDayKey.split("\\|", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Expected stationDayKey format stationId|yyyy-MM-dd, got " + stationDayKey);
        }
        return new StationMonthKey(parts[0], YearMonth.from(LocalDate.parse(parts[1]))).asKey();
    }

    private static boolean isUsableObservation(NoaaObservation observation) {
        return observation.stationId() != null
            && observation.observationDate() != null
            && observation.temperatureCelsius() != null
            && observation.temperatureQualityCode() != null
            && !observation.temperatureQualityCode().equals("9");
    }

    private static String stationDayKey(NoaaObservation observation) {
        return observation.stationId() + "|" + observation.observationDate();
    }
}
