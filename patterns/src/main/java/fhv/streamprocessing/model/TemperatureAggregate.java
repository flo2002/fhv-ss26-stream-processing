package fhv.streamprocessing.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class TemperatureAggregate {
    private long count;
    private double sum;

    public TemperatureAggregate() {
    }

    public TemperatureAggregate add(Double value) {
        if (value != null) {
            count++;
            sum += value;
        }
        return this;
    }

    public long getCount() {
        return count;
    }

    public double getSum() {
        return sum;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    @JsonIgnore
    public Double averageTemperatureCelsius() {
        return count == 0 ? null : sum / count;
    }
}
