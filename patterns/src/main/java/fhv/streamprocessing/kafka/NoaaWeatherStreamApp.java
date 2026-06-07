package fhv.streamprocessing.kafka;

import fhv.streamprocessing.dashboard.DashboardSink;
import fhv.streamprocessing.dashboard.PostgresDashboardSink;
import fhv.streamprocessing.dashboard.StationMetadataLoader;
import fhv.streamprocessing.model.NoaaObservation;
import fhv.streamprocessing.pattern1.temperature.DailyAverageTemperatureTopology;
import fhv.streamprocessing.pattern1.temperature.TemperatureAggregate;
import fhv.streamprocessing.pattern10.blizzard.BlizzardDetectionTopology;
import fhv.streamprocessing.pattern10.blizzard.BlizzardEvent;
import fhv.streamprocessing.pattern2.frostdays.MonthlyFrostDaysTopology;
import fhv.streamprocessing.pattern3.rapidchange.RapidTemperatureChangeEvent;
import fhv.streamprocessing.pattern3.rapidchange.RapidTemperatureChangeTopology;
import fhv.streamprocessing.pattern5.rainduration.RainDurationAggregate;
import fhv.streamprocessing.pattern5.rainduration.YearlyRainDurationTopology;
import fhv.streamprocessing.pattern6.temperatureranking.AnnualPeakTemperatureRankingTopology;
import fhv.streamprocessing.pattern6.temperatureranking.TemperatureRankingAggregate;
import fhv.streamprocessing.serde.JsonSerde;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

public final class NoaaWeatherStreamApp {
    private NoaaWeatherStreamApp() {
    }

    public static void main(String[] args) {
        AppConfig config = AppConfig.fromEnvironment();
        DashboardSink dashboardSink = createDashboardSink(config);
        Topology topology = buildTopology(config, dashboardSink);
        KafkaStreams streams = new KafkaStreams(topology, streamsProperties(config));
        CountDownLatch shutdownLatch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close(Duration.ofSeconds(10));
            dashboardSink.close();
            shutdownLatch.countDown();
        }));

        streams.setUncaughtExceptionHandler(exception -> {
            exception.printStackTrace();
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });

        streams.start();
        System.out.printf(
            "NOAA Kafka stream started. bootstrap=%s applicationId=%s topics=%s patterns=%s%n",
            config.bootstrapServers(),
            config.applicationId(),
            config.inputTopics(),
            config.streamPatterns()
        );

        try {
            shutdownLatch.await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    public static Topology buildTopology(AppConfig config) {
        return buildTopology(config, DashboardSink.noop());
    }

    public static Topology buildTopology(AppConfig config, DashboardSink dashboardSink) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, NoaaObservation> observations = parsedNoaaObservationStream(builder, config.inputTopics(), dashboardSink);

        observations.peek((key, observation) -> System.out.printf(
            "station=%s day=%s observedAt=%s tempC=%s windMps=%s rainDurationHours=%s sourceKey=%s%n",
            observation.stationId(),
            observation.observationDate(),
            observation.observedAt(),
            observation.temperatureCelsius(),
            observation.windSpeedMetersPerSecond(),
            observation.rainDurationHours(),
            key
        ));

        if (config.runsPattern(StreamPattern.TEMPERATURE)) {
            KTable<String, TemperatureAggregate> dailyTemperatureAverages = DailyAverageTemperatureTopology.build(observations);
            dailyTemperatureAverages
                .toStream()
                .peek(dashboardSink::recordDailyAverage)
                .mapValues(TemperatureAggregate::averageTemperatureCelsius)
                .to(config.dailyAverageTopic(), Produced.with(Serdes.String(), Serdes.Double()));
        }

        if (config.runsPattern(StreamPattern.TEMPERATURE_RANKING)) {
            AnnualPeakTemperatureRankingTopology.build(observations, config.temperatureRankingYear())
                .toStream()
                .peek(dashboardSink::recordTemperatureWindowRanking)
                .to(config.dailyTemperatureRankingTopic(), Produced.with(Serdes.String(), new JsonSerde<>(TemperatureRankingAggregate.class)));
        }

        if (config.runsPattern(StreamPattern.RAIN_DURATION)) {
            YearlyRainDurationTopology.build(observations, config.rainDurationYear())
                .toStream()
                .peek(dashboardSink::recordYearlyRainDuration)
                .mapValues(RainDurationAggregate::averageDurationHours)
                .to(config.yearlyRainDurationTopic(), Produced.with(Serdes.String(), Serdes.Double()));
        }

        if (config.runsPattern(StreamPattern.FROST_DAYS)) {
            MonthlyFrostDaysTopology.build(observations, config.frostCountYear())
                .toStream()
                .peek(dashboardSink::recordMonthlyFrostDays)
                .to(config.monthlyFrostDaysTopic(), Produced.with(Serdes.String(), Serdes.Long()));
        }

        if (config.runsPattern(StreamPattern.RAPID_TEMPERATURE_CHANGE)) {
            RapidTemperatureChangeTopology.build(
                observations,
                config.rapidChangeYear(),
                config.rapidChangeWindowHours(),
                config.rapidChangeGraceMinutes()
            )
                .peek(dashboardSink::recordRapidTemperatureChange)
                .to(config.rapidChangeEventsTopic(), Produced.with(Serdes.String(), new JsonSerde<>(RapidTemperatureChangeEvent.class)));
        }

        if (config.runsPattern(StreamPattern.BLIZZARD)) {
            BlizzardDetectionTopology.build(
                observations,
                config.blizzardYear(),
                config.blizzardFreezingThresholdCelsius(),
                config.blizzardWindThresholdMetersPerSecond(),
                config.blizzardWindowHours(),
                config.blizzardAdvanceHours(),
                config.blizzardGraceMinutes()
            )
                .peek(dashboardSink::recordBlizzardEvent)
                .to(config.blizzardEventsTopic(), Produced.with(Serdes.String(), new JsonSerde<>(BlizzardEvent.class)));
        }

        return builder.build();
    }

    private static KStream<String, NoaaObservation> parsedNoaaObservationStream(
        StreamsBuilder builder,
        List<String> inputTopics,
        DashboardSink dashboardSink
    ) {
        return builder
            .stream(
                inputTopics,
                Consumed.with(Serdes.String(), new JsonSerde<>(NoaaKafkaMessage.class))
                    .withTimestampExtractor(new NoaaObservedAtTimestampExtractor())
            )
            .peek((key, value) -> dashboardSink.incrementRawRequests())
            .mapValues(NoaaKafkaMessage::toObservation)
            .peek((key, value) -> dashboardSink.incrementParsedRequests());
    }

    private static Properties streamsProperties(AppConfig config) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, config.applicationId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1_000);
        return props;
    }

    private static DashboardSink createDashboardSink(AppConfig config) {
        if (!config.dashboardEnabled()) {
            return DashboardSink.noop();
        }
        return new PostgresDashboardSink(
            config.dashboardJdbcUrl(),
            config.dashboardDbUser(),
            config.dashboardDbPassword(),
            config.stationHistoryUrl()
        );
    }

    public enum StreamPattern {
        TEMPERATURE("temperature"),
        RAIN_DURATION("rain-duration"),
        FROST_DAYS("frost-days"),
        TEMPERATURE_RANKING("temperature-ranking"),
        RAPID_TEMPERATURE_CHANGE("rapid-temperature-change"),
        BLIZZARD("blizzard");

        private final String configValue;

        StreamPattern(String configValue) {
            this.configValue = configValue;
        }

        static StreamPattern fromConfigValue(String value) {
            String normalized = value.trim().toLowerCase().replace('_', '-');
            return Arrays.stream(values())
                .filter(pattern -> pattern.configValue.equals(normalized))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                    "Unknown stream pattern '" + value + "'. Supported values: temperature, rain-duration, frost-days, temperature-ranking, rapid-temperature-change, blizzard, all"
                ));
        }

        String configValue() {
            return configValue;
        }
    }

    public record AppConfig(
        String bootstrapServers,
        String applicationId,
        List<String> inputTopics,
        Set<StreamPattern> streamPatterns,
        String dailyAverageTopic,
        String yearlyRainDurationTopic,
        String monthlyFrostDaysTopic,
        String dailyTemperatureRankingTopic,
        String blizzardEventsTopic,
        String rapidChangeEventsTopic,
        int rainDurationYear,
        int frostCountYear,
        int temperatureRankingYear,
        int temperatureRankingWindowHours,
        int temperatureRankingAdvanceMinutes,
        int temperatureRankingGraceMinutes,
        int blizzardYear,
        int blizzardWindowHours,
        int blizzardAdvanceHours,
        int blizzardGraceMinutes,
        double blizzardFreezingThresholdCelsius,
        double blizzardWindThresholdMetersPerSecond,
        int rapidChangeYear,
        int rapidChangeWindowHours,
        int rapidChangeGraceMinutes,
        boolean dashboardEnabled,
        String dashboardJdbcUrl,
        String dashboardDbUser,
        String dashboardDbPassword,
        String stationHistoryUrl
    ) {
        public static AppConfig fromEnvironment() {
            String bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:19094");
            List<String> inputTopics = topics(env("KAFKA_INPUT_TOPICS", "noaa.weather.raw"));
            Set<StreamPattern> streamPatterns = streamPatterns(env("STREAM_PATTERN", "all"));
            String dailyAverageTopic = env("KAFKA_DAILY_AVERAGE_TOPIC", "noaa.weather.daily-average-temperature");
            String yearlyRainDurationTopic = env("KAFKA_YEARLY_RAIN_DURATION_TOPIC", "noaa.weather.yearly-average-rain-duration");
            String monthlyFrostDaysTopic = env("KAFKA_MONTHLY_FROST_DAYS_TOPIC", "noaa.weather.monthly-frost-days");
            String dailyTemperatureRankingTopic = env("KAFKA_DAILY_TEMPERATURE_RANKING_TOPIC", "noaa.weather.daily-temperature-ranking");
            String blizzardEventsTopic = env("KAFKA_BLIZZARD_EVENTS_TOPIC", "noaa.weather.blizzard-events");
            String rapidChangeEventsTopic = env("KAFKA_RAPID_CHANGE_EVENTS_TOPIC", "noaa.weather.rapid-temperature-change");
            int rainDurationYear = envInt("RAIN_DURATION_YEAR", 2025);
            int frostCountYear = envInt("FROST_COUNT_YEAR", 2025);
            int temperatureRankingYear = envInt("TEMPERATURE_RANKING_YEAR", 2025);
            int temperatureRankingWindowHours = envInt("TEMPERATURE_RANKING_WINDOW_HOURS", 24);
            int temperatureRankingAdvanceMinutes = envInt("TEMPERATURE_RANKING_ADVANCE_MINUTES", 60);
            int temperatureRankingGraceMinutes = envInt("TEMPERATURE_RANKING_GRACE_MINUTES", 10);
            int blizzardYear = envInt("BLIZZARD_YEAR", 2025);
            int blizzardWindowHours = envInt("BLIZZARD_WINDOW_HOURS", 24);
            int blizzardAdvanceHours = envInt("BLIZZARD_ADVANCE_HOURS", 1);
            int blizzardGraceMinutes = envInt("BLIZZARD_GRACE_MINUTES", 10);
            double blizzardFreezingThresholdCelsius = envDouble("BLIZZARD_FREEZING_THRESHOLD_C", 0.0);
            double blizzardWindThresholdMetersPerSecond = envDouble("BLIZZARD_WIND_THRESHOLD_MPS", 12.0);
            int rapidChangeYear = envInt("RAPID_CHANGE_YEAR", 2025);
            int rapidChangeWindowHours = envInt("RAPID_CHANGE_WINDOW_HOURS", 24);
            int rapidChangeGraceMinutes = envInt("RAPID_CHANGE_GRACE_MINUTES", 10);

            return new AppConfig(
                bootstrapServers,
                applicationId(
                    inputTopics,
                    streamPatterns,
                    dailyAverageTopic,
                    yearlyRainDurationTopic,
                    monthlyFrostDaysTopic,
                    dailyTemperatureRankingTopic,
                    blizzardEventsTopic,
                    rapidChangeEventsTopic,
                    rainDurationYear,
                    frostCountYear,
                    temperatureRankingYear,
                    temperatureRankingWindowHours,
                    temperatureRankingAdvanceMinutes,
                    temperatureRankingGraceMinutes,
                    blizzardYear,
                    blizzardWindowHours,
                    blizzardAdvanceHours,
                    blizzardGraceMinutes,
                    blizzardFreezingThresholdCelsius,
                    blizzardWindThresholdMetersPerSecond,
                    rapidChangeYear,
                    rapidChangeWindowHours,
                    rapidChangeGraceMinutes
                ),
                inputTopics,
                streamPatterns,
                dailyAverageTopic,
                yearlyRainDurationTopic,
                monthlyFrostDaysTopic,
                dailyTemperatureRankingTopic,
                blizzardEventsTopic,
                rapidChangeEventsTopic,
                rainDurationYear,
                frostCountYear,
                temperatureRankingYear,
                temperatureRankingWindowHours,
                temperatureRankingAdvanceMinutes,
                temperatureRankingGraceMinutes,
                blizzardYear,
                blizzardWindowHours,
                blizzardAdvanceHours,
                blizzardGraceMinutes,
                blizzardFreezingThresholdCelsius,
                blizzardWindThresholdMetersPerSecond,
                rapidChangeYear,
                rapidChangeWindowHours,
                rapidChangeGraceMinutes,
                envBoolean("DASHBOARD_SINK_ENABLED", true),
                env("DASHBOARD_JDBC_URL", "jdbc:postgresql://localhost:5432/noaa"),
                env("DASHBOARD_DB_USER", "noaa"),
                env("DASHBOARD_DB_PASSWORD", "noaa"),
                env("NOAA_ISD_STATION_HISTORY_URL", StationMetadataLoader.DEFAULT_STATION_HISTORY_URL)
            );
        }

        public boolean runsPattern(StreamPattern pattern) {
            return streamPatterns.contains(pattern);
        }

        private static String env(String name, String defaultValue) {
            String value = System.getenv(name);
            return value == null || value.isBlank() ? defaultValue : value;
        }

        private static String applicationId(
            List<String> inputTopics,
            Set<StreamPattern> streamPatterns,
            String dailyAverageTopic,
            String yearlyRainDurationTopic,
            String monthlyFrostDaysTopic,
            String dailyTemperatureRankingTopic,
            String blizzardEventsTopic,
            String rapidChangeEventsTopic,
            int rainDurationYear,
            int frostCountYear,
            int temperatureRankingYear,
            int temperatureRankingWindowHours,
            int temperatureRankingAdvanceMinutes,
            int temperatureRankingGraceMinutes,
            int blizzardYear,
            int blizzardWindowHours,
            int blizzardAdvanceHours,
            int blizzardGraceMinutes,
            double blizzardFreezingThresholdCelsius,
            double blizzardWindThresholdMetersPerSecond,
            int rapidChangeYear,
            int rapidChangeWindowHours,
            int rapidChangeGraceMinutes
        ) {
            String configuredApplicationId = env("KAFKA_STREAMS_APPLICATION_ID", "");
            if (!configuredApplicationId.isBlank()) {
                return configuredApplicationId;
            }

            String prefix = env("KAFKA_STREAMS_APPLICATION_ID_PREFIX", "noaa-weather");
            return generatedApplicationId(
                prefix,
                inputTopics,
                streamPatterns,
                dailyAverageTopic,
                yearlyRainDurationTopic,
                monthlyFrostDaysTopic,
                dailyTemperatureRankingTopic,
                blizzardEventsTopic,
                rapidChangeEventsTopic,
                rainDurationYear,
                frostCountYear,
                temperatureRankingYear,
                temperatureRankingWindowHours,
                temperatureRankingAdvanceMinutes,
                temperatureRankingGraceMinutes,
                blizzardYear,
                blizzardWindowHours,
                blizzardAdvanceHours,
                blizzardGraceMinutes,
                blizzardFreezingThresholdCelsius,
                blizzardWindThresholdMetersPerSecond,
                rapidChangeYear,
                rapidChangeWindowHours,
                rapidChangeGraceMinutes
            );
        }

        static String generatedApplicationId(
            String prefix,
            List<String> inputTopics,
            Set<StreamPattern> streamPatterns,
            String dailyAverageTopic,
            String yearlyRainDurationTopic,
            String monthlyFrostDaysTopic,
            int rainDurationYear,
            int frostCountYear
        ) {
            return generatedApplicationId(
                prefix,
                inputTopics,
                streamPatterns,
                dailyAverageTopic,
                yearlyRainDurationTopic,
                monthlyFrostDaysTopic,
                "noaa.weather.daily-temperature-ranking",
                "noaa.weather.blizzard-events",
                "noaa.weather.rapid-temperature-change",
                rainDurationYear,
                frostCountYear,
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

        static String generatedApplicationId(
            String prefix,
            List<String> inputTopics,
            Set<StreamPattern> streamPatterns,
            String dailyAverageTopic,
            String yearlyRainDurationTopic,
            int rainDurationYear
        ) {
            return generatedApplicationId(
                prefix,
                inputTopics,
                streamPatterns,
                dailyAverageTopic,
                yearlyRainDurationTopic,
                "noaa.weather.monthly-frost-days",
                "noaa.weather.daily-temperature-ranking",
                "noaa.weather.blizzard-events",
                "noaa.weather.rapid-temperature-change",
                rainDurationYear,
                2025,
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

        static String generatedApplicationId(
            String prefix,
            List<String> inputTopics,
            Set<StreamPattern> streamPatterns,
            String dailyAverageTopic,
            String yearlyRainDurationTopic,
            String monthlyFrostDaysTopic,
            String dailyTemperatureRankingTopic,
            String blizzardEventsTopic,
            String rapidChangeEventsTopic,
            int rainDurationYear,
            int frostCountYear,
            int temperatureRankingYear,
            int temperatureRankingWindowHours,
            int temperatureRankingAdvanceMinutes,
            int temperatureRankingGraceMinutes,
            int blizzardYear,
            int blizzardWindowHours,
            int blizzardAdvanceHours,
            int blizzardGraceMinutes,
            double blizzardFreezingThresholdCelsius,
            double blizzardWindThresholdMetersPerSecond,
            int rapidChangeYear,
            int rapidChangeWindowHours,
            int rapidChangeGraceMinutes
        ) {
            String sourceTopics = inputTopics.stream()
                .sorted()
                .collect(Collectors.joining("-"));
            String patternTopics = streamPatterns.stream()
                .sorted(Comparator.comparing(StreamPattern::configValue))
                .map(pattern -> patternReplayKey(
                    pattern,
                    dailyAverageTopic,
                    yearlyRainDurationTopic,
                    monthlyFrostDaysTopic,
                    dailyTemperatureRankingTopic,
                    blizzardEventsTopic,
                    rapidChangeEventsTopic,
                    rainDurationYear,
                    frostCountYear,
                    temperatureRankingYear,
                    temperatureRankingWindowHours,
                    temperatureRankingAdvanceMinutes,
                    temperatureRankingGraceMinutes,
                    blizzardYear,
                    blizzardWindowHours,
                    blizzardAdvanceHours,
                    blizzardGraceMinutes,
                    blizzardFreezingThresholdCelsius,
                    blizzardWindThresholdMetersPerSecond,
                    rapidChangeYear,
                    rapidChangeWindowHours,
                    rapidChangeGraceMinutes
                ))
                .collect(Collectors.joining("-"));
            String replayKey = sourceTopics + "-" + patternTopics;
            String patternNames = streamPatterns.stream()
                .sorted(Comparator.comparing(StreamPattern::configValue))
                .map(StreamPattern::configValue)
                .collect(Collectors.joining("-"));

            return sanitizeApplicationId(prefix + "-" + patternNames + "-" + shortHash(replayKey));
        }

        private static String patternReplayKey(
            StreamPattern pattern,
            String dailyAverageTopic,
            String yearlyRainDurationTopic,
            String monthlyFrostDaysTopic,
            String dailyTemperatureRankingTopic,
            String blizzardEventsTopic,
            String rapidChangeEventsTopic,
            int rainDurationYear,
            int frostCountYear,
            int temperatureRankingYear,
            int temperatureRankingWindowHours,
            int temperatureRankingAdvanceMinutes,
            int temperatureRankingGraceMinutes,
            int blizzardYear,
            int blizzardWindowHours,
            int blizzardAdvanceHours,
            int blizzardGraceMinutes,
            double blizzardFreezingThresholdCelsius,
            double blizzardWindThresholdMetersPerSecond,
            int rapidChangeYear,
            int rapidChangeWindowHours,
            int rapidChangeGraceMinutes
        ) {
            String key = pattern.configValue() + "-" + outputTopic(
                pattern,
                dailyAverageTopic,
                yearlyRainDurationTopic,
                monthlyFrostDaysTopic,
                dailyTemperatureRankingTopic,
                blizzardEventsTopic,
                rapidChangeEventsTopic
            );
            if (pattern == StreamPattern.RAIN_DURATION) {
                return key + "-" + rainDurationYear;
            }
            if (pattern == StreamPattern.FROST_DAYS) {
                return key + "-" + frostCountYear;
            }
            if (pattern == StreamPattern.TEMPERATURE_RANKING) {
                return key + "-" + temperatureRankingYear + "-annual-peak-v2";
            }
            if (pattern == StreamPattern.BLIZZARD) {
                return key
                    + "-" + blizzardYear
                    + "-wh" + blizzardWindowHours
                    + "-ah" + blizzardAdvanceHours
                    + "-gm" + blizzardGraceMinutes
                    + "-ft" + decimalReplayKey(blizzardFreezingThresholdCelsius)
                    + "-wt" + decimalReplayKey(blizzardWindThresholdMetersPerSecond)
                    + "-v1";
            }
            if (pattern == StreamPattern.RAPID_TEMPERATURE_CHANGE) {
                return key
                    + "-" + rapidChangeYear
                    + "-wh" + rapidChangeWindowHours
                    + "-gm" + rapidChangeGraceMinutes
                    + "-v1";
            }
            return key;
        }

        private static String outputTopic(
            StreamPattern pattern,
            String dailyAverageTopic,
            String yearlyRainDurationTopic,
            String monthlyFrostDaysTopic,
            String dailyTemperatureRankingTopic,
            String blizzardEventsTopic,
            String rapidChangeEventsTopic
        ) {
            return switch (pattern) {
                case TEMPERATURE -> dailyAverageTopic;
                case RAIN_DURATION -> yearlyRainDurationTopic;
                case FROST_DAYS -> monthlyFrostDaysTopic;
                case TEMPERATURE_RANKING -> dailyTemperatureRankingTopic;
                case BLIZZARD -> blizzardEventsTopic;
                case RAPID_TEMPERATURE_CHANGE -> rapidChangeEventsTopic;
            };
        }

        private static String sanitizeApplicationId(String value) {
            String sanitized = value.toLowerCase(Locale.ROOT)
                .replaceAll("[^a-z0-9._-]+", "-")
                .replaceAll("-+", "-")
                .replaceAll("(^[-.]+|[-.]+$)", "");
            if (sanitized.isBlank()) {
                return "noaa-weather-patterns";
            }
            return sanitized;
        }

        private static String shortHash(String value) {
            try {
                byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
                StringBuilder hash = new StringBuilder();
                for (int index = 0; index < 4; index++) {
                    hash.append(String.format("%02x", digest[index]));
                }
                return hash.toString();
            } catch (NoSuchAlgorithmException exception) {
                throw new IllegalStateException("SHA-256 is not available", exception);
            }
        }

        private static List<String> topics(String value) {
            return Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(topic -> !topic.isEmpty())
                .toList();
        }

        private static String decimalReplayKey(double value) {
            return String.format(Locale.ROOT, "%.1f", value).replace('.', '_');
        }

        private static boolean envBoolean(String name, boolean defaultValue) {
            String value = System.getenv(name);
            if (value == null || value.isBlank()) {
                return defaultValue;
            }
            return Boolean.parseBoolean(value);
        }

        private static Set<StreamPattern> streamPatterns(String value) {
            if (value == null || value.isBlank() || value.equalsIgnoreCase("all")) {
                return EnumSet.allOf(StreamPattern.class);
            }

            EnumSet<StreamPattern> patterns = EnumSet.noneOf(StreamPattern.class);
            Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(pattern -> !pattern.isEmpty())
                .map(StreamPattern::fromConfigValue)
                .forEach(patterns::add);

            if (patterns.isEmpty()) {
                return EnumSet.allOf(StreamPattern.class);
            }
            return patterns;
        }

        private static double envDouble(String name, double defaultValue) {
            String value = System.getenv(name);
            if (value == null || value.isBlank()) {
                return defaultValue;
            }
            return Double.parseDouble(value);
        }

        private static int envInt(String name, int defaultValue) {
            String value = System.getenv(name);
            if (value == null || value.isBlank()) {
                return defaultValue;
            }
            return Integer.parseInt(value);
        }
    }
}
