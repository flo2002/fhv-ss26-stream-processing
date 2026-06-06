package fhv.streamprocessing.kafka;

import fhv.streamprocessing.model.NoaaObservation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class NoaaObservedAtTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        Object value = record.value();

        if (value instanceof NoaaKafkaMessage message && message.observedAt() != null) {
            return message.observedAt().toInstant().toEpochMilli();
        }

        if (value instanceof NoaaObservation observation && observation.observedAt() != null) {
            return observation.observedAt().toInstant().toEpochMilli();
        }

        return record.timestamp();
    }
}
