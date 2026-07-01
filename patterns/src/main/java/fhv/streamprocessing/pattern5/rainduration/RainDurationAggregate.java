package fhv.streamprocessing.pattern5.rainduration;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Mutable Kafka Streams state used to incrementally calculate rain duration.
 */
public class RainDurationAggregate {
    private long count;
    private long totalDurationHours;

    public RainDurationAggregate() {
    }

    public RainDurationAggregate add(Integer durationHours) {
        // Kafka persists both values, allowing the average to continue exactly
        // after a restart instead of averaging already-rounded averages.
        count++;
        totalDurationHours += durationHours;
        return this;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getTotalDurationHours() {
        return totalDurationHours;
    }

    public void setTotalDurationHours(long totalDurationHours) {
        this.totalDurationHours = totalDurationHours;
    }

    @JsonIgnore
    public double averageDurationHours() {
        return count == 0 ? 0.0 : (double) totalDurationHours / count;
    }
}
