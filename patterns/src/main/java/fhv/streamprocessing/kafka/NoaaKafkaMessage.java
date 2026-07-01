package fhv.streamprocessing.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import fhv.streamprocessing.model.IsdRecordParser;
import fhv.streamprocessing.model.NoaaObservation;
import java.time.OffsetDateTime;

@JsonIgnoreProperties(ignoreUnknown = true)
/**
 * Transport model for raw NOAA records received from the Kafka input topic.
 */
public record NoaaKafkaMessage(
    @JsonProperty("schema_version") int schemaVersion,
    Source source,
    @JsonProperty("observed_at") OffsetDateTime observedAt,
    @JsonProperty("ingested_at") OffsetDateTime ingestedAt,
    @JsonProperty("record_number") long recordNumber,
    @JsonProperty("payload_format") String payloadFormat,
    @JsonProperty("payload_text") String payloadText
) {
    public NoaaObservation toObservation() {
        return IsdRecordParser.parse(payloadText, observedAt, source == null ? null : source.path(), recordNumber);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Source(
        String type,
        String host,
        String path,
        @JsonProperty("file_name") String fileName,
        String year
    ) {
    }
}
