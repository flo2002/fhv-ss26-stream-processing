package fhv.streamprocessing.pattern1.temperature;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class TemperatureAggregate {
    private long count;
    private double sum;

    public TemperatureAggregate() {
    }

    public TemperatureAggregate add(Double value) {
        count++;
        sum += value;
        return this;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    @JsonIgnore
    public double averageTemperatureCelsius() {
        return count == 0 ? 0.0 : sum / count;
    }
}
