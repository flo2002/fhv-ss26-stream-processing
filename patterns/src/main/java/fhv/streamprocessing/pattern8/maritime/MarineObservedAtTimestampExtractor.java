package fhv.streamprocessing.pattern8.maritime;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class MarineObservedAtTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        Object value = record.value();

        if (value instanceof AisPositionEvent event && event.observedAt() != null) {
            return event.observedAt().toEpochMilli();
        }

        if (value instanceof BuoyObservationEvent event && event.observedAt() != null) {
            return event.observedAt().toEpochMilli();
        }

        return record.timestamp();
    }
}
