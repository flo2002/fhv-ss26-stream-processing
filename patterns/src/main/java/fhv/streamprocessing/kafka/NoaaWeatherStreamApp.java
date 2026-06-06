package fhv.streamprocessing.kafka;

import fhv.streamprocessing.dashboard.DashboardSink;
import fhv.streamprocessing.dashboard.PostgresDashboardSink;
import fhv.streamprocessing.dashboard.StationMetadataLoader;
import fhv.streamprocessing.model.NoaaObservation;
import fhv.streamprocessing.model.RainDurationAggregate;
import fhv.streamprocessing.model.StationWindowTemperatureStats;
import fhv.streamprocessing.model.TemperatureAggregate;
import fhv.streamprocessing.model.TemperatureRankingAggregate;
import fhv.streamprocessing.model.TemperatureWindowStats;
import fhv.streamprocessing.serde.JsonSerde;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
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
        KTable<String, TemperatureAggregate> dailyTemperatureAverages = null;
        KStream<String, NoaaObservation> usableTemperatureObservations = null;

        observations.peek((key, observation) -> System.out.printf(
            "station=%s day=%s observedAt=%s tempC=%s rainDurationHours=%s sourceKey=%s%n",
            observation.stationId(),
            observation.observationDate(),
            observation.observedAt(),
            observation.temperatureCelsius(),
            observation.rainDurationHours(),
            key
        ));

        if (config.runsPattern(StreamPattern.TEMPERATURE) || config.runsPattern(StreamPattern.TEMPERATURE_RANKING)) {
            usableTemperatureObservations = observations
                .filter((key, observation) -> observation.isUsableForTemperatureAverages());

            if (config.runsPattern(StreamPattern.TEMPERATURE)) {
                dailyTemperatureAverages = dailyAverageTemperatures(usableTemperatureObservations);
            }
        }

        if (config.runsPattern(StreamPattern.TEMPERATURE) && dailyTemperatureAverages != null) {
            dailyTemperatureAverages
                .toStream()
                .peek(dashboardSink::recordDailyAverage)
                .mapValues(TemperatureAggregate::averageTemperatureCelsius)
                .to(config.dailyAverageTopic(), Produced.with(Serdes.String(), Serdes.Double()));
        }

        if (config.runsPattern(StreamPattern.TEMPERATURE_RANKING) && usableTemperatureObservations != null) {
            temperatureWindowRankings(
                usableTemperatureObservations,
                config.temperatureRankingYear()
            )
                .toStream()
                .peek(dashboardSink::recordTemperatureWindowRanking)
                .to(config.dailyTemperatureRankingTopic(), Produced.with(Serdes.String(), new JsonSerde<>(TemperatureRankingAggregate.class)));
        }

        if (config.runsPattern(StreamPattern.RAIN_DURATION)) {
            KStream<String, NoaaObservation> rainDurationObservations = observations
                .filter((key, observation) -> observation.isUsableForRainDurationAverages())
                .filter((key, observation) -> observation.observationDate().getYear() == config.rainDurationYear());

            yearlyAverageRainDurations(rainDurationObservations)
                .toStream()
                .peek(dashboardSink::recordYearlyRainDuration)
                .mapValues(RainDurationAggregate::averageDurationHours)
                .to(config.yearlyRainDurationTopic(), Produced.with(Serdes.String(), Serdes.Double()));
        }

        if (config.runsPattern(StreamPattern.FROST_DAYS)) {
            KStream<String, NoaaObservation> frostObservations = observations
                .filter((key, observation) -> observation.isUsableForTemperatureAverages())
                .filter((key, observation) -> observation.observationDate().getYear() == config.frostCountYear())
                .filter((key, observation) -> observation.temperatureCelsius() < 0.0);

            monthlyFrostDayCounts(frostObservations)
                .toStream()
                .peek(dashboardSink::recordMonthlyFrostDays)
                .to(config.monthlyFrostDaysTopic(), Produced.with(Serdes.String(), Serdes.Long()));
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

    public static KTable<String, TemperatureAggregate> dailyAverageTemperatures(KStream<String, NoaaObservation> observations) {
        KGroupedStream<String, NoaaObservation> groupedByStationDay = observations
            .map((key, observation) -> KeyValue.pair(observation.stationDayKey(), observation))
            .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(NoaaObservation.class)));

        return groupedByStationDay.aggregate(
            TemperatureAggregate::new,
            (stationDay, observation, aggregate) -> aggregate.add(observation.temperatureCelsius()),
            Materialized.with(Serdes.String(), new JsonSerde<>(TemperatureAggregate.class))
        );
    }

    public static KTable<String, RainDurationAggregate> yearlyAverageRainDurations(KStream<String, NoaaObservation> observations) {
        KGroupedStream<String, NoaaObservation> groupedByStationYear = observations
            .map((key, observation) -> KeyValue.pair(observation.stationYearKey(), observation))
            .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(NoaaObservation.class)));

        return groupedByStationYear.aggregate(
            RainDurationAggregate::new,
            (stationYear, observation, aggregate) -> aggregate.add(observation.rainDurationHours()),
            Materialized.with(Serdes.String(), new JsonSerde<>(RainDurationAggregate.class))
        );
    }

    public static KTable<String, Long> monthlyFrostDayCounts(KStream<String, NoaaObservation> observations) {
        KTable<String, Long> frostObservationsPerStationDay = observations
            .map((key, observation) -> KeyValue.pair(observation.stationDayKey(), observation))
            .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(NoaaObservation.class)))
            .count(Materialized.with(Serdes.String(), Serdes.Long()));

        KGroupedTable<String, Long> groupedByStationMonth = frostObservationsPerStationDay
            .groupBy(
                (stationDayKey, frostObservations) -> KeyValue.pair(
                    stationMonthKey(stationDayKey),
                    frostObservations > 0 ? 1L : 0L
                ),
                Grouped.with(Serdes.String(), Serdes.Long())
            );

        return groupedByStationMonth.aggregate(
            () -> 0L,
            (stationMonth, newValue, aggregate) -> aggregate + newValue,
            (stationMonth, oldValue, aggregate) -> aggregate - oldValue,
            Materialized.with(Serdes.String(), Serdes.Long())
        );
    }

    public static KTable<String, TemperatureRankingAggregate> temperatureWindowRankings(
        KStream<String, NoaaObservation> observations,
        int rankingYear
    ) {
        long windowStartEpochMs = rankingYearWindowStartEpochMs(rankingYear);
        long windowEndEpochMs = rankingYearWindowEndEpochMs(rankingYear);

        return observations
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
                rankingWindowKey(windowStartEpochMs, windowEndEpochMs),
                new StationWindowTemperatureStats(
                    stationId,
                    windowStartEpochMs,
                    windowEndEpochMs,
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

    private static long rankingYearWindowStartEpochMs(int rankingYear) {
        return LocalDate.of(rankingYear, 1, 1)
            .atStartOfDay()
            .toInstant(ZoneOffset.UTC)
            .toEpochMilli();
    }

    private static long rankingYearWindowEndEpochMs(int rankingYear) {
        return LocalDate.of(rankingYear + 1, 1, 1)
            .atStartOfDay()
            .toInstant(ZoneOffset.UTC)
            .toEpochMilli();
    }

    static String rankingWindowKey(long windowStartEpochMs, long windowEndEpochMs) {
        return windowStartEpochMs + "|" + windowEndEpochMs;
    }

    static String stationMonthKey(String stationDayKey) {
        DashboardSink.StationDay stationDay = DashboardSink.stationDay(stationDayKey);
        return stationDay.stationId() + "|" + YearMonth.from(stationDay.day());
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
        TEMPERATURE_RANKING("temperature-ranking");

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
                    "Unknown stream pattern '" + value + "'. Supported values: temperature, rain-duration, frost-days, temperature-ranking, all"
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
        int rainDurationYear,
        int frostCountYear,
        int temperatureRankingYear,
        int temperatureRankingWindowHours,
        int temperatureRankingAdvanceMinutes,
        int temperatureRankingGraceMinutes,
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
            int rainDurationYear = envInt("RAIN_DURATION_YEAR", 2025);
            int frostCountYear = envInt("FROST_COUNT_YEAR", 2025);
            int temperatureRankingYear = envInt("TEMPERATURE_RANKING_YEAR", 2025);
            int temperatureRankingWindowHours = envInt("TEMPERATURE_RANKING_WINDOW_HOURS", 24);
            int temperatureRankingAdvanceMinutes = envInt("TEMPERATURE_RANKING_ADVANCE_MINUTES", 60);
            int temperatureRankingGraceMinutes = envInt("TEMPERATURE_RANKING_GRACE_MINUTES", 10);

            return new AppConfig(
                bootstrapServers,
                applicationId(
                    inputTopics,
                    streamPatterns,
                    dailyAverageTopic,
                    yearlyRainDurationTopic,
                    monthlyFrostDaysTopic,
                    dailyTemperatureRankingTopic,
                    rainDurationYear,
                    frostCountYear,
                    temperatureRankingYear,
                    temperatureRankingWindowHours,
                    temperatureRankingAdvanceMinutes,
                    temperatureRankingGraceMinutes
                ),
                inputTopics,
                streamPatterns,
                dailyAverageTopic,
                yearlyRainDurationTopic,
                monthlyFrostDaysTopic,
                dailyTemperatureRankingTopic,
                rainDurationYear,
                frostCountYear,
                temperatureRankingYear,
                temperatureRankingWindowHours,
                temperatureRankingAdvanceMinutes,
                temperatureRankingGraceMinutes,
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
            int rainDurationYear,
            int frostCountYear,
            int temperatureRankingYear,
            int temperatureRankingWindowHours,
            int temperatureRankingAdvanceMinutes,
            int temperatureRankingGraceMinutes
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
                rainDurationYear,
                frostCountYear,
                temperatureRankingYear,
                temperatureRankingWindowHours,
                temperatureRankingAdvanceMinutes,
                temperatureRankingGraceMinutes
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
                rainDurationYear,
                frostCountYear,
                2025,
                24,
                60,
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
                rainDurationYear,
                2025,
                2025,
                24,
                60,
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
            int rainDurationYear,
            int frostCountYear,
            int temperatureRankingYear,
            int temperatureRankingWindowHours,
            int temperatureRankingAdvanceMinutes,
            int temperatureRankingGraceMinutes
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
                    rainDurationYear,
                    frostCountYear,
                    temperatureRankingYear,
                    temperatureRankingWindowHours,
                    temperatureRankingAdvanceMinutes,
                    temperatureRankingGraceMinutes
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
            int rainDurationYear,
            int frostCountYear,
            int temperatureRankingYear,
            int temperatureRankingWindowHours,
            int temperatureRankingAdvanceMinutes,
            int temperatureRankingGraceMinutes
        ) {
            String key = pattern.configValue() + "-" + outputTopic(
                pattern,
                dailyAverageTopic,
                yearlyRainDurationTopic,
                monthlyFrostDaysTopic,
                dailyTemperatureRankingTopic
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
            return key;
        }

        private static String outputTopic(
            StreamPattern pattern,
            String dailyAverageTopic,
            String yearlyRainDurationTopic,
            String monthlyFrostDaysTopic,
            String dailyTemperatureRankingTopic
        ) {
            return switch (pattern) {
                case TEMPERATURE -> dailyAverageTopic;
                case RAIN_DURATION -> yearlyRainDurationTopic;
                case FROST_DAYS -> monthlyFrostDaysTopic;
                case TEMPERATURE_RANKING -> dailyTemperatureRankingTopic;
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

        private static int envInt(String name, int defaultValue) {
            String value = System.getenv(name);
            if (value == null || value.isBlank()) {
                return defaultValue;
            }
            return Integer.parseInt(value);
        }
    }
}
