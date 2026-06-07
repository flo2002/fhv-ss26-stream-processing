package fhv.streamprocessing.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import fhv.streamprocessing.model.NoaaObservation;
import fhv.streamprocessing.pattern10.blizzard.BlizzardDetectionTopology;
import fhv.streamprocessing.pattern10.blizzard.BlizzardEvent;
import fhv.streamprocessing.pattern10.blizzard.StationBlizzardWindowKey;
import fhv.streamprocessing.pattern2.frostdays.MonthlyFrostDaysTopology;
import fhv.streamprocessing.pattern6.temperatureranking.AnnualPeakTemperatureRankingTopology;
import fhv.streamprocessing.pattern6.temperatureranking.RankingWindowKey;
import fhv.streamprocessing.pattern6.temperatureranking.TemperatureRankingAggregate;
import fhv.streamprocessing.serde.JsonSerde;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.Test;

class NoaaWeatherStreamAppTest {
    @Test
    void generatedApplicationIdIsStableForSameRawInputsAndPatternOutputs() {
        String first = testGeneratedApplicationId(
            "NOAA Weather Dashboard",
            List.of("noaa.weather.raw.backfill", "noaa.weather.raw"),
            EnumSet.of(
                NoaaWeatherStreamApp.StreamPattern.RAIN_DURATION,
                NoaaWeatherStreamApp.StreamPattern.TEMPERATURE
            ),
            "noaa.weather.daily-average-temperature",
            "noaa.weather.yearly-average-rain-duration",
            "noaa.weather.monthly-frost-days",
            2025,
            2025
        );

        String second = testGeneratedApplicationId(
            "noaa-weather-dashboard",
            List.of("noaa.weather.raw", "noaa.weather.raw.backfill"),
            EnumSet.of(
                NoaaWeatherStreamApp.StreamPattern.TEMPERATURE,
                NoaaWeatherStreamApp.StreamPattern.RAIN_DURATION
            ),
            "noaa.weather.daily-average-temperature",
            "noaa.weather.yearly-average-rain-duration",
            "noaa.weather.monthly-frost-days",
            2025,
            2025
        );

        assertEquals(first, second);
    }

    @Test
    void generatedApplicationIdChangesWhenPatternOutputTopicChanges() {
        String original = testGeneratedApplicationId(
            "noaa-weather-dashboard",
            List.of("noaa.weather.raw"),
            EnumSet.of(NoaaWeatherStreamApp.StreamPattern.RAIN_DURATION),
            "noaa.weather.daily-average-temperature",
            "noaa.weather.yearly-average-rain-duration",
            "noaa.weather.monthly-frost-days",
            2025,
            2025
        );

        String newPatternTopic = testGeneratedApplicationId(
            "noaa-weather-dashboard",
            List.of("noaa.weather.raw"),
            EnumSet.of(NoaaWeatherStreamApp.StreamPattern.RAIN_DURATION),
            "noaa.weather.daily-average-temperature",
            "noaa.weather.yearly-average-rain-duration-v2",
            "noaa.weather.monthly-frost-days",
            2025,
            2025
        );

        assertNotEquals(original, newPatternTopic);
    }

    @Test
    void generatedApplicationIdChangesWhenRainDurationYearChanges() {
        String original = testGeneratedApplicationId(
            "noaa-weather-dashboard",
            List.of("noaa.weather.raw"),
            EnumSet.of(NoaaWeatherStreamApp.StreamPattern.RAIN_DURATION),
            "noaa.weather.daily-average-temperature",
            "noaa.weather.yearly-average-rain-duration",
            "noaa.weather.monthly-frost-days",
            2025,
            2025
        );

        String differentRainYear = testGeneratedApplicationId(
            "noaa-weather-dashboard",
            List.of("noaa.weather.raw"),
            EnumSet.of(NoaaWeatherStreamApp.StreamPattern.RAIN_DURATION),
            "noaa.weather.daily-average-temperature",
            "noaa.weather.yearly-average-rain-duration",
            "noaa.weather.monthly-frost-days",
            2024,
            2025
        );

        assertNotEquals(original, differentRainYear);
    }

    @Test
    void generatedApplicationIdStaysShortEnoughForKafkaStreamsInternalTopics() {
        String applicationId = testGeneratedApplicationId(
            "noaa-weather-dashboard",
            List.of("noaa.weather.raw"),
            EnumSet.of(NoaaWeatherStreamApp.StreamPattern.RAIN_DURATION),
            "noaa.weather.daily-average-temperature",
            "noaa.weather.yearly-average-rain-duration",
            "noaa.weather.monthly-frost-days",
            2025,
            2025
        );

        assertTrue(applicationId.length() < 80);
    }

    @Test
    void generatedApplicationIdChangesWhenFrostOutputTopicChanges() {
        String original = testGeneratedApplicationId(
            "noaa-weather-dashboard",
            List.of("noaa.weather.raw"),
            EnumSet.of(NoaaWeatherStreamApp.StreamPattern.FROST_DAYS),
            "noaa.weather.daily-average-temperature",
            "noaa.weather.yearly-average-rain-duration",
            "noaa.weather.monthly-frost-days",
            2025,
            2025
        );

        String newPatternTopic = testGeneratedApplicationId(
            "noaa-weather-dashboard",
            List.of("noaa.weather.raw"),
            EnumSet.of(NoaaWeatherStreamApp.StreamPattern.FROST_DAYS),
            "noaa.weather.daily-average-temperature",
            "noaa.weather.yearly-average-rain-duration",
            "noaa.weather.monthly-frost-days-v2",
            2025,
            2025
        );

        assertNotEquals(original, newPatternTopic);
    }

    @Test
    void generatedApplicationIdChangesWhenFrostYearChanges() {
        String original = testGeneratedApplicationId(
            "noaa-weather-dashboard",
            List.of("noaa.weather.raw"),
            EnumSet.of(NoaaWeatherStreamApp.StreamPattern.FROST_DAYS),
            "noaa.weather.daily-average-temperature",
            "noaa.weather.yearly-average-rain-duration",
            "noaa.weather.monthly-frost-days",
            2025,
            2025
        );

        String differentFrostYear = testGeneratedApplicationId(
            "noaa-weather-dashboard",
            List.of("noaa.weather.raw"),
            EnumSet.of(NoaaWeatherStreamApp.StreamPattern.FROST_DAYS),
            "noaa.weather.daily-average-temperature",
            "noaa.weather.yearly-average-rain-duration",
            "noaa.weather.monthly-frost-days",
            2025,
            2024
        );

        assertNotEquals(original, differentFrostYear);
    }

    @Test
    void generatedApplicationIdChangesWhenTemperatureRankingTopicChanges() {
        String original = fullGeneratedApplicationId(
            EnumSet.of(NoaaWeatherStreamApp.StreamPattern.TEMPERATURE_RANKING),
            "noaa.weather.daily-temperature-ranking",
            "noaa.weather.blizzard-events",
            2025,
            24,
            1,
            10,
            0.0,
            12.0
        );

        String newPatternTopic = fullGeneratedApplicationId(
            EnumSet.of(NoaaWeatherStreamApp.StreamPattern.TEMPERATURE_RANKING),
            "noaa.weather.daily-temperature-ranking-v2",
            "noaa.weather.blizzard-events",
            2025,
            24,
            1,
            10,
            0.0,
            12.0
        );

        assertNotEquals(original, newPatternTopic);
    }

    @Test
    void generatedApplicationIdChangesWhenTemperatureRankingYearChanges() {
        String original = fullGeneratedApplicationId(
            EnumSet.of(NoaaWeatherStreamApp.StreamPattern.TEMPERATURE_RANKING),
            "noaa.weather.daily-temperature-ranking",
            "noaa.weather.blizzard-events",
            2025,
            24,
            1,
            10,
            0.0,
            12.0
        );

        String differentRankingYear = testGeneratedApplicationIdFull(
            "noaa-weather-dashboard",
            List.of("noaa.weather.raw"),
            EnumSet.of(NoaaWeatherStreamApp.StreamPattern.TEMPERATURE_RANKING),
            "noaa.weather.daily-average-temperature",
            "noaa.weather.yearly-average-rain-duration",
            "noaa.weather.monthly-frost-days",
            "noaa.weather.daily-temperature-ranking",
            "noaa.weather.blizzard-events",
            "noaa.weather.rapid-temperature-change",
            2025,
            2025,
            2024,
            24,
            60,
            10,
            2025,
            24,
            1,
            10,
            0.0,
            12.0,
            2025,
            24,
            10
        );

        assertNotEquals(original, differentRankingYear);
    }

    @Test
    void generatedApplicationIdStaysStableWhenUnusedTemperatureRankingWindowSettingsChange() {
        String original = fullGeneratedApplicationId(
            EnumSet.of(NoaaWeatherStreamApp.StreamPattern.TEMPERATURE_RANKING),
            "noaa.weather.daily-temperature-ranking",
            "noaa.weather.blizzard-events",
            2025,
            24,
            1,
            10,
            0.0,
            12.0
        );

        String differentWindow = testGeneratedApplicationIdFull(
            "noaa-weather-dashboard",
            List.of("noaa.weather.raw"),
            EnumSet.of(NoaaWeatherStreamApp.StreamPattern.TEMPERATURE_RANKING),
            "noaa.weather.daily-average-temperature",
            "noaa.weather.yearly-average-rain-duration",
            "noaa.weather.monthly-frost-days",
            "noaa.weather.daily-temperature-ranking",
            "noaa.weather.blizzard-events",
            "noaa.weather.rapid-temperature-change",
            2025,
            2025,
            2025,
            12,
            30,
            5,
            2025,
            24,
            1,
            10,
            0.0,
            12.0,
            2025,
            24,
            10
        );

        assertEquals(original, differentWindow);
    }

    @Test
    void generatedApplicationIdChangesWhenBlizzardThresholdChanges() {
        String original = fullGeneratedApplicationId(
            EnumSet.of(NoaaWeatherStreamApp.StreamPattern.BLIZZARD),
            "noaa.weather.daily-temperature-ranking",
            "noaa.weather.blizzard-events",
            2025,
            24,
            1,
            10,
            0.0,
            12.0
        );

        String strongerWindThreshold = fullGeneratedApplicationId(
            EnumSet.of(NoaaWeatherStreamApp.StreamPattern.BLIZZARD),
            "noaa.weather.daily-temperature-ranking",
            "noaa.weather.blizzard-events",
            2025,
            24,
            1,
            10,
            0.0,
            14.0
        );

        assertNotEquals(original, strongerWindThreshold);
    }

    @Test
    void monthlyFrostDayCountsCountDistinctDaysPerStationMonth() {
        StreamsBuilder builder = new StreamsBuilder();
        MonthlyFrostDaysTopology.build(
            builder.stream("observations", Consumed.with(Serdes.String(), new JsonSerde<>(NoaaObservation.class))),
            2025
        )
            .toStream()
            .to("monthly-frost-days", Produced.with(Serdes.String(), Serdes.Long()));

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "monthly-frost-days-test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), properties)) {
            TestInputTopic<String, NoaaObservation> inputTopic = driver.createInputTopic(
                "observations",
                new StringSerializer(),
                new JsonSerde<>(NoaaObservation.class).serializer()
            );
            TestOutputTopic<String, Long> outputTopic = driver.createOutputTopic(
                "monthly-frost-days",
                new StringDeserializer(),
                new LongDeserializer()
            );

            inputTopic.pipeInput("record-1", observation("010010-99999", LocalDate.of(2025, 1, 3), -5.0));
            inputTopic.pipeInput("record-2", observation("010010-99999", LocalDate.of(2025, 1, 3), -3.5));
            inputTopic.pipeInput("record-3", observation("010010-99999", LocalDate.of(2025, 1, 4), -1.0));
            inputTopic.pipeInput("record-4", observation("010010-99999", LocalDate.of(2025, 2, 1), -2.0));
            inputTopic.pipeInput("record-5", observation("020020-88888", LocalDate.of(2025, 1, 9), -4.0));

            Map<String, Long> latestCounts = new LinkedHashMap<>();
            outputTopic.readKeyValuesToList().forEach(record -> latestCounts.put(record.key, record.value));

            assertEquals(2L, latestCounts.get("010010-99999|2025-01"));
            assertEquals(1L, latestCounts.get("010010-99999|2025-02"));
            assertEquals(1L, latestCounts.get("020020-88888|2025-01"));
        }
    }

    @Test
    void temperatureWindowRankingsTrackHottestAndColdestStationsForAnnualPeakWindow() {
        StreamsBuilder builder = new StreamsBuilder();
        AnnualPeakTemperatureRankingTopology.build(
            builder.stream("observations", Consumed.with(Serdes.String(), new JsonSerde<>(NoaaObservation.class))),
            2025
        )
            .toStream()
            .to("daily-temperature-ranking", Produced.with(Serdes.String(), new JsonSerde<>(TemperatureRankingAggregate.class)));

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "daily-temperature-ranking-test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), properties)) {
            TestInputTopic<String, NoaaObservation> inputTopic = driver.createInputTopic(
                "observations",
                new StringSerializer(),
                new JsonSerde<>(NoaaObservation.class).serializer()
            );
            TestOutputTopic<String, TemperatureRankingAggregate> outputTopic = driver.createOutputTopic(
                "daily-temperature-ranking",
                new StringDeserializer(),
                new JsonSerde<>(TemperatureRankingAggregate.class).deserializer()
            );

            inputTopic.pipeInput("record-1", observation("010010-99999", LocalDateTime.of(2025, 1, 3, 0, 0), 10.0), Instant.parse("2025-01-03T00:00:00Z"));
            inputTopic.pipeInput("record-2", observation("020020-88888", LocalDateTime.of(2025, 1, 3, 0, 30), -2.0), Instant.parse("2025-01-03T00:30:00Z"));
            inputTopic.pipeInput("record-3", observation("010010-99999", LocalDateTime.of(2025, 7, 3, 1, 30), 20.0), Instant.parse("2025-07-03T01:30:00Z"));
            inputTopic.pipeInput("record-4", observation("030030-77777", LocalDateTime.of(2025, 1, 3, 1, 45), 18.0), Instant.parse("2025-01-03T01:45:00Z"));
            inputTopic.pipeInput("record-5", observation("040040-66666", LocalDateTime.of(2024, 12, 31, 23, 0), 30.0), Instant.parse("2024-12-31T23:00:00Z"));

            Map<String, TemperatureRankingAggregate> latestRankings = new LinkedHashMap<>();
            outputTopic.readKeyValuesToList().forEach(record -> latestRankings.put(record.key, record.value));

            String annualWindowKey = new RankingWindowKey(
                Instant.parse("2025-01-01T00:00:00Z").toEpochMilli(),
                Instant.parse("2026-01-01T00:00:00Z").toEpochMilli()
            ).asKey();
            TemperatureRankingAggregate ranking = latestRankings.get(annualWindowKey);

            assertEquals("010010-99999", ranking.hottestStations(3).get(0).stationId());
            assertEquals(20.0, ranking.hottestStations(3).get(0).maxTemperatureCelsius());
            assertEquals("020020-88888", ranking.coldestStations(3).get(0).stationId());
            assertEquals(-2.0, ranking.coldestStations(3).get(0).minTemperatureCelsius());
            assertEquals(3, ranking.getStationsById().size());

            String previousYearWindowKey = new RankingWindowKey(
                Instant.parse("2024-12-31T23:00:00Z").toEpochMilli(),
                Instant.parse("2025-01-01T01:00:00Z").toEpochMilli()
            ).asKey();
            assertTrue(!latestRankings.containsKey(previousYearWindowKey));
        }
    }

    @Test
    void blizzardDetectionCombinesTemperatureWindAndPrecipitationWithinTheSameWindow() {
        StreamsBuilder builder = new StreamsBuilder();
        BlizzardDetectionTopology.build(
            builder.stream("observations", Consumed.with(Serdes.String(), new JsonSerde<>(NoaaObservation.class))),
            2025,
            0.0,
            12.0,
            24,
            24,
            10
        )
            .to("blizzard-events", Produced.with(Serdes.String(), new JsonSerde<>(BlizzardEvent.class)));

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "blizzard-detection-test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), properties)) {
            TestInputTopic<String, NoaaObservation> inputTopic = driver.createInputTopic(
                "observations",
                new StringSerializer(),
                new JsonSerde<>(NoaaObservation.class).serializer()
            );
            TestOutputTopic<String, BlizzardEvent> outputTopic = driver.createOutputTopic(
                "blizzard-events",
                new StringDeserializer(),
                new JsonSerde<>(BlizzardEvent.class).deserializer()
            );

            inputTopic.pipeInput(
                "record-1",
                observation("010010-99999", LocalDateTime.of(2025, 1, 1, 1, 0), -4.0, null, null),
                Instant.parse("2025-01-01T01:00:00Z")
            );
            inputTopic.pipeInput(
                "record-2",
                observation("010010-99999", LocalDateTime.of(2025, 1, 1, 5, 0), null, 13.2, null),
                Instant.parse("2025-01-01T05:00:00Z")
            );
            inputTopic.pipeInput(
                "record-3",
                observation("010010-99999", LocalDateTime.of(2025, 1, 1, 6, 0), null, null, 3),
                Instant.parse("2025-01-01T06:00:00Z")
            );

            var detectedEvents = outputTopic.readKeyValuesToList();
            assertEquals(1, detectedEvents.size());

            StationBlizzardWindowKey detectedKey = StationBlizzardWindowKey.parse(detectedEvents.get(0).key);
            BlizzardEvent detectedEvent = detectedEvents.get(0).value;

            assertEquals("010010-99999", detectedEvent.stationId());
            assertEquals(0.0, detectedEvent.freezingThresholdCelsius());
            assertEquals(12.0, detectedEvent.strongWindThresholdMetersPerSecond());
            assertEquals(-4.0, detectedEvent.minTemperatureCelsius());
            assertEquals(13.2, detectedEvent.maxWindSpeedMetersPerSecond());
            assertEquals(1L, detectedEvent.precipitationObservationCount());
            assertTrue(detectedEvent.isDetected());
            assertEquals(Instant.parse("2025-01-01T00:00:00Z"), detectedKey.windowStart());
            assertEquals(Instant.parse("2025-01-02T00:00:00Z"), detectedKey.windowEnd());
        }
    }

    private String testGeneratedApplicationId(
        String prefix,
        List<String> inputTopics,
        Set<NoaaWeatherStreamApp.StreamPattern> patterns,
        String dailyAvgTopic,
        String yearlyRainTopic,
        String monthlyFrostTopic,
        int rainYear,
        int frostYear
    ) {
        return NoaaWeatherStreamApp.AppConfig.generatedApplicationId(
            prefix,
            inputTopics,
            patterns,
            dailyAvgTopic,
            yearlyRainTopic,
            monthlyFrostTopic,
            "noaa.weather.daily-temperature-ranking",
            "noaa.weather.blizzard-events",
            "noaa.weather.rapid-temperature-change",
            rainYear,
            frostYear,
            2025,
            24,
            60,
            10,
            2025,
            24,
            1,
            10,
            0.0,
            12.0,
            2025,
            24,
            10
        );
    }

    private String testGeneratedApplicationIdFull(
        String prefix,
        List<String> inputTopics,
        Set<NoaaWeatherStreamApp.StreamPattern> patterns,
        String dailyAvgTopic,
        String yearlyRainTopic,
        String monthlyFrostTopic,
        String rankingTopic,
        String blizzardTopic,
        String rapidChangeTopic,
        int rainYear,
        int frostYear,
        int rankingYear,
        int rankingWh,
        int rankingAm,
        int rankingGm,
        int blizzardYear,
        int blizzardWh,
        int blizzardAh,
        int blizzardGm,
        double blizzardFt,
        double blizzardWt,
        int rapidYear,
        int rapidWh,
        int rapidGm
    ) {
        return NoaaWeatherStreamApp.AppConfig.generatedApplicationId(
            prefix,
            inputTopics,
            patterns,
            dailyAvgTopic,
            yearlyRainTopic,
            monthlyFrostTopic,
            rankingTopic,
            blizzardTopic,
            rapidChangeTopic,
            rainYear,
            frostYear,
            rankingYear,
            rankingWh,
            rankingAm,
            rankingGm,
            blizzardYear,
            blizzardWh,
            blizzardAh,
            blizzardGm,
            blizzardFt,
            blizzardWt,
            rapidYear,
            rapidWh,
            rapidGm
        );
    }

    private static String fullGeneratedApplicationId(
        EnumSet<NoaaWeatherStreamApp.StreamPattern> patterns,
        String temperatureRankingTopic,
        String blizzardTopic,
        int blizzardYear,
        int blizzardWindowHours,
        int blizzardAdvanceHours,
        int blizzardGraceMinutes,
        double blizzardFreezingThresholdCelsius,
        double blizzardWindThresholdMetersPerSecond
    ) {
        return NoaaWeatherStreamApp.AppConfig.generatedApplicationId(
            "noaa-weather-dashboard",
            List.of("noaa.weather.raw"),
            patterns,
            "noaa.weather.daily-average-temperature",
            "noaa.weather.yearly-average-rain-duration",
            "noaa.weather.monthly-frost-days",
            temperatureRankingTopic,
            blizzardTopic,
            "noaa.weather.rapid-temperature-change",
            2025,
            2025,
            2025,
            24,
            60,
            10,
            blizzardYear,
            blizzardWindowHours,
            blizzardAdvanceHours,
            blizzardGraceMinutes,
            blizzardFreezingThresholdCelsius,
            blizzardWindThresholdMetersPerSecond,
            2025,
            24,
            10
        );
    }

    private static NoaaObservation observation(String stationId, LocalDate observationDate, double temperatureCelsius) {
        return new NoaaObservation(
            stationId,
            observationDate,
            observationDate.atStartOfDay().atOffset(ZoneOffset.UTC),
            temperatureCelsius,
            "1",
            null,
            null,
            null,
            "/tmp/source",
            1L,
            "raw"
        );
    }

    private static NoaaObservation observation(String stationId, LocalDateTime observedAt, double temperatureCelsius) {
        OffsetDateTime offsetDateTime = observedAt.atOffset(ZoneOffset.UTC);
        return new NoaaObservation(
            stationId,
            observedAt.toLocalDate(),
            offsetDateTime,
            temperatureCelsius,
            "1",
            null,
            null,
            null,
            "/tmp/source",
            1L,
            "raw"
        );
    }

    private static NoaaObservation observation(
        String stationId,
        LocalDateTime observedAt,
        Double temperatureCelsius,
        Double windSpeedMetersPerSecond,
        Integer rainDurationHours
    ) {
        OffsetDateTime offsetDateTime = observedAt.atOffset(ZoneOffset.UTC);
        return new NoaaObservation(
            stationId,
            observedAt.toLocalDate(),
            offsetDateTime,
            temperatureCelsius,
            temperatureCelsius == null ? null : "1",
            windSpeedMetersPerSecond,
            windSpeedMetersPerSecond == null ? null : "1",
            rainDurationHours,
            "/tmp/source",
            1L,
            "raw"
        );
    }
}
