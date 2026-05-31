package fhv.streamprocessing.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.EnumSet;
import java.util.List;
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
}
