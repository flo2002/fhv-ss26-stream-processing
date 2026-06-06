package fhv.streamprocessing.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import fhv.streamprocessing.model.NoaaObservation;
import fhv.streamprocessing.serde.JsonSerde;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
        String first = NoaaWeatherStreamApp.AppConfig.generatedApplicationId(
            "NOAA Weather Dashboard",
            List.of("noaa.weather.raw.backfill", "noaa.weather.raw"),
            EnumSet.of(
                NoaaWeatherStreamApp.StreamPattern.RAIN_DURATION,
                NoaaWeatherStreamApp.StreamPattern.TEMPERATURE
            ),
            "noaa.weather.daily-average-temperature",
            "noaa.weather.yearly-average-rain-duration",
            2025
        );

        String second = NoaaWeatherStreamApp.AppConfig.generatedApplicationId(
            "noaa-weather-dashboard",
            List.of("noaa.weather.raw", "noaa.weather.raw.backfill"),
            EnumSet.of(
                NoaaWeatherStreamApp.StreamPattern.TEMPERATURE,
                NoaaWeatherStreamApp.StreamPattern.RAIN_DURATION
            ),
            "noaa.weather.daily-average-temperature",
            "noaa.weather.yearly-average-rain-duration",
            2025
        );

        assertEquals(first, second);
    }

    @Test
    void generatedApplicationIdChangesWhenPatternOutputTopicChanges() {
        String original = NoaaWeatherStreamApp.AppConfig.generatedApplicationId(
            "noaa-weather-dashboard",
            List.of("noaa.weather.raw"),
            EnumSet.of(NoaaWeatherStreamApp.StreamPattern.RAIN_DURATION),
            "noaa.weather.daily-average-temperature",
            "noaa.weather.yearly-average-rain-duration",
            2025
        );

        String newPatternTopic = NoaaWeatherStreamApp.AppConfig.generatedApplicationId(
            "noaa-weather-dashboard",
            List.of("noaa.weather.raw"),
            EnumSet.of(NoaaWeatherStreamApp.StreamPattern.RAIN_DURATION),
            "noaa.weather.daily-average-temperature",
            "noaa.weather.yearly-average-rain-duration-v2",
            2025
        );

        assertNotEquals(original, newPatternTopic);
    }

    @Test
    void generatedApplicationIdChangesWhenRainDurationYearChanges() {
        String original = NoaaWeatherStreamApp.AppConfig.generatedApplicationId(
            "noaa-weather-dashboard",
            List.of("noaa.weather.raw"),
            EnumSet.of(NoaaWeatherStreamApp.StreamPattern.RAIN_DURATION),
            "noaa.weather.daily-average-temperature",
            "noaa.weather.yearly-average-rain-duration",
            2025
        );

        String differentRainYear = NoaaWeatherStreamApp.AppConfig.generatedApplicationId(
            "noaa-weather-dashboard",
            List.of("noaa.weather.raw"),
            EnumSet.of(NoaaWeatherStreamApp.StreamPattern.RAIN_DURATION),
            "noaa.weather.daily-average-temperature",
            "noaa.weather.yearly-average-rain-duration",
            2024
        );

        assertNotEquals(original, differentRainYear);
    }

    @Test
    void generatedApplicationIdStaysShortEnoughForKafkaStreamsInternalTopics() {
        String applicationId = NoaaWeatherStreamApp.AppConfig.generatedApplicationId(
            "noaa-weather-dashboard",
            List.of("noaa.weather.raw"),
            EnumSet.of(NoaaWeatherStreamApp.StreamPattern.RAIN_DURATION),
            "noaa.weather.daily-average-temperature",
            "noaa.weather.yearly-average-rain-duration",
            2025
        );

        assertTrue(applicationId.length() < 80);
    }

    @Test
    void generatedApplicationIdChangesWhenFrostOutputTopicChanges() {
        String original = NoaaWeatherStreamApp.AppConfig.generatedApplicationId(
            "noaa-weather-dashboard",
            List.of("noaa.weather.raw"),
            EnumSet.of(NoaaWeatherStreamApp.StreamPattern.FROST_DAYS),
            "noaa.weather.daily-average-temperature",
            "noaa.weather.yearly-average-rain-duration",
            "noaa.weather.monthly-frost-days",
            2025,
            2025
        );

        String newPatternTopic = NoaaWeatherStreamApp.AppConfig.generatedApplicationId(
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
        String original = NoaaWeatherStreamApp.AppConfig.generatedApplicationId(
            "noaa-weather-dashboard",
            List.of("noaa.weather.raw"),
            EnumSet.of(NoaaWeatherStreamApp.StreamPattern.FROST_DAYS),
            "noaa.weather.daily-average-temperature",
            "noaa.weather.yearly-average-rain-duration",
            "noaa.weather.monthly-frost-days",
            2025,
            2025
        );

        String differentFrostYear = NoaaWeatherStreamApp.AppConfig.generatedApplicationId(
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
    void monthlyFrostDayCountsCountDistinctDaysPerStationMonth() {
        StreamsBuilder builder = new StreamsBuilder();
        NoaaWeatherStreamApp.monthlyFrostDayCounts(
            builder.stream("observations", Consumed.with(Serdes.String(), new JsonSerde<>(NoaaObservation.class)))
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

    private static NoaaObservation observation(String stationId, LocalDate observationDate, double temperatureCelsius) {
        return new NoaaObservation(
            stationId,
            observationDate,
            observationDate.atStartOfDay().atOffset(ZoneOffset.UTC),
            temperatureCelsius,
            "1",
            null,
            "/tmp/source",
            1L,
            "raw"
        );
    }
}
