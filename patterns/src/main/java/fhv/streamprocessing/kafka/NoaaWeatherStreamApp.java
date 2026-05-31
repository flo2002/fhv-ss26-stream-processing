package fhv.streamprocessing.kafka;

import fhv.streamprocessing.dashboard.DashboardSink;
import fhv.streamprocessing.dashboard.PostgresDashboardSink;
import fhv.streamprocessing.dashboard.StationMetadataLoader;
import fhv.streamprocessing.model.NoaaObservation;
import fhv.streamprocessing.model.RainDurationAggregate;
import fhv.streamprocessing.model.TemperatureAggregate;
import fhv.streamprocessing.serde.JsonSerde;
import java.time.Duration;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
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
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
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
            "NOAA Kafka stream started. bootstrap=%s topics=%s patterns=%s%n",
            config.bootstrapServers(),
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
            "station=%s day=%s observedAt=%s tempC=%s rainDurationHours=%s sourceKey=%s%n",
            observation.stationId(),
            observation.observationDate(),
            observation.observedAt(),
            observation.temperatureCelsius(),
            observation.rainDurationHours(),
            key
        ));

        if (config.runsPattern(StreamPattern.TEMPERATURE)) {
            KStream<String, NoaaObservation> temperatureObservations = observations
                .filter((key, observation) -> observation.isUsableForTemperatureAverages());

            dailyAverageTemperatures(temperatureObservations)
                .toStream()
                .peek(dashboardSink::recordDailyAverage)
                .mapValues(TemperatureAggregate::averageTemperatureCelsius)
                .to(config.dailyAverageTopic(), Produced.with(Serdes.String(), Serdes.Double()));
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

        return builder.build();
    }

    private static KStream<String, NoaaObservation> parsedNoaaObservationStream(
        StreamsBuilder builder,
        List<String> inputTopics,
        DashboardSink dashboardSink
    ) {
        return builder
            .stream(inputTopics, Consumed.with(Serdes.String(), new JsonSerde<>(NoaaKafkaMessage.class)))
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
        RAIN_DURATION("rain-duration");

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
                    "Unknown stream pattern '" + value + "'. Supported values: temperature, rain-duration, all"
                ));
        }
    }

    public record AppConfig(
        String bootstrapServers,
        String applicationId,
        List<String> inputTopics,
        Set<StreamPattern> streamPatterns,
        String dailyAverageTopic,
        String yearlyRainDurationTopic,
        int rainDurationYear,
        boolean dashboardEnabled,
        String dashboardJdbcUrl,
        String dashboardDbUser,
        String dashboardDbPassword,
        String stationHistoryUrl
    ) {
        public static AppConfig fromEnvironment() {
            return new AppConfig(
                env("KAFKA_BOOTSTRAP_SERVERS", "localhost:19094"),
                env("KAFKA_STREAMS_APPLICATION_ID", "noaa-weather-patterns"),
                topics(env("KAFKA_INPUT_TOPICS", "noaa.weather.raw")),
                streamPatterns(env("STREAM_PATTERN", "all")),
                env("KAFKA_DAILY_AVERAGE_TOPIC", "noaa.weather.daily-average-temperature"),
                env("KAFKA_YEARLY_RAIN_DURATION_TOPIC", "noaa.weather.yearly-average-rain-duration"),
                envInt("RAIN_DURATION_YEAR", 2025),
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
