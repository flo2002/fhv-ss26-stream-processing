package fhv.streamprocessing.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class RainDurationAggregate {
    private long count;
    private long totalDurationHours;

    public RainDurationAggregate() {
    }

    public RainDurationAggregate add(Integer durationHours) {
        if (durationHours != null) {
            count++;
            totalDurationHours += durationHours;
        }
        return this;
    }

    public long getCount() {
        return count;
    }

    public long getTotalDurationHours() {
        return totalDurationHours;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public void setTotalDurationHours(long totalDurationHours) {
        this.totalDurationHours = totalDurationHours;
    }

    @JsonIgnore
    public Double averageDurationHours() {
        return count == 0 ? null : (double) totalDurationHours / count;
    }
}
