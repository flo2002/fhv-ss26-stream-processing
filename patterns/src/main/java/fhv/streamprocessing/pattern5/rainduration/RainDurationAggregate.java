package fhv.streamprocessing.pattern5.rainduration;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class RainDurationAggregate {
    private long count;
    private long totalDurationHours;

    public RainDurationAggregate() {
    }

    public RainDurationAggregate add(Integer durationHours) {
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
