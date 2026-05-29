package fhv.streamprocessing.kafka;

import fhv.streamprocessing.model.NoaaObservation;
import fhv.streamprocessing.model.TemperatureAggregate;
import fhv.streamprocessing.serde.JsonSerde;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
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
        Topology topology = buildTopology(config);
        KafkaStreams streams = new KafkaStreams(topology, streamsProperties(config));
        CountDownLatch shutdownLatch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close(Duration.ofSeconds(10));
            shutdownLatch.countDown();
        }));

        streams.setUncaughtExceptionHandler(exception -> {
            exception.printStackTrace();
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });

        streams.start();
        System.out.printf("NOAA Kafka stream started. bootstrap=%s topics=%s%n", config.bootstrapServers(), config.inputTopics());

        try {
            shutdownLatch.await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    public static Topology buildTopology(AppConfig config) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, NoaaObservation> observations = noaaObservationStream(builder, config.inputTopics());

        observations.peek((key, observation) -> System.out.printf(
            "station=%s day=%s observedAt=%s tempC=%s sourceKey=%s%n",
            observation.stationId(),
            observation.observationDate(),
            observation.observedAt(),
            observation.temperatureCelsius(),
            key
        ));

        dailyAverageTemperatures(observations)
            .toStream()
            .mapValues(TemperatureAggregate::averageTemperatureCelsius)
            .to(config.dailyAverageTopic(), Produced.with(Serdes.String(), Serdes.Double()));

        return builder.build();
    }

    public static KStream<String, NoaaObservation> noaaObservationStream(StreamsBuilder builder, List<String> inputTopics) {
        return builder
            .stream(inputTopics, Consumed.with(Serdes.String(), new JsonSerde<>(NoaaKafkaMessage.class)))
            .mapValues(NoaaKafkaMessage::toObservation)
            .filter((key, observation) -> observation.isUsableForTemperatureAverages());
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

    public record AppConfig(
        String bootstrapServers,
        String applicationId,
        List<String> inputTopics,
        String dailyAverageTopic
    ) {
        public static AppConfig fromEnvironment() {
            return new AppConfig(
                env("KAFKA_BOOTSTRAP_SERVERS", "localhost:19094"),
                env("KAFKA_STREAMS_APPLICATION_ID", "noaa-weather-patterns"),
                topics(env("KAFKA_INPUT_TOPICS", "noaa.weather.raw")),
                env("KAFKA_DAILY_AVERAGE_TOPIC", "noaa.weather.daily-average-temperature")
            );
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
    }
}
