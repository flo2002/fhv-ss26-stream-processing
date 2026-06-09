package fhv.streamprocessing.pattern9.wetdry;

import fhv.streamprocessing.model.NoaaObservation;
import fhv.streamprocessing.serde.JsonSerde;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.StoreBuilder;

public final class WetDryPeriodTopology {
    private static final String STATE_STORE_NAME = "wet-dry-period-state";

    private WetDryPeriodTopology() {
    }

    public static StoreBuilder<KeyValueStore<String, WetPeriodState>> stateStore() {
        return Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(STATE_STORE_NAME),
            Serdes.String(),
            new JsonSerde<>(WetPeriodState.class)
        );
    }

    public static KStream<String, WetPeriodEvent> build(KStream<String, NoaaObservation> observations, int year) {
        return observations
            .filter((key, observation) -> isUsableObservation(observation, year))
            .selectKey((key, observation) -> observation.stationId())
            .transform(() -> new WetDryPeriodTransformer(), Named.as("wet-dry-period-detector"), STATE_STORE_NAME)
            .filter((key, event) -> event != null);
    }

    private static boolean isUsableObservation(NoaaObservation observation, int year) {
        return observation != null
            && observation.stationId() != null
            && observation.observedAt() != null
            && observation.observedAt().atZoneSameInstant(ZoneOffset.UTC).getYear() == year;
    }

    private static boolean isWet(NoaaObservation observation) {
        return observation.rainDurationHours() != null && observation.rainDurationHours() > 0;
    }

    private static final class WetDryPeriodTransformer implements Transformer<String, NoaaObservation, KeyValue<String, WetPeriodEvent>> {
        private KeyValueStore<String, WetPeriodState> stateStore;

        @Override
        public void init(ProcessorContext context) {
            stateStore = context.getStateStore(STATE_STORE_NAME);
        }

        @Override
        public KeyValue<String, WetPeriodEvent> transform(String stationId, NoaaObservation observation) {
            Instant observedAt = observation.observedAt().toInstant();
            WetPeriodState state = stateStore.get(stationId);

            if (isWet(observation)) {
                if (state == null) {
                    stateStore.put(stationId, new WetPeriodState(stationId, observedAt, 1));
                } else {
                    stateStore.put(stationId, state.addWetObservation());
                }
                return null;
            }

            if (state == null) {
                return null;
            }

            stateStore.delete(stationId);
            WetPeriodEvent event = new WetPeriodEvent(
                stationId,
                state.periodStart(),
                observedAt,
                Duration.between(state.periodStart(), observedAt).toMinutes(),
                state.precipitationObservationCount(),
                observedAt
            );
            return KeyValue.pair(stationId + "-" + state.periodStart(), event);
        }

        @Override
        public void close() {
        }
    }
}
