package fhv.streamprocessing.pattern1.temperature;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Stores the running sum and count required for one station-day temperature average.
 */
public class TemperatureAggregate {
    private long count;
    private double sum;

    public TemperatureAggregate() {
    }

    public TemperatureAggregate add(Double value) {
        // Kafka Streams restores these two fields from the state-store changelog
        // and calls add() once for every new temperature in this station-day.
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
        // Derive the average from stored sum and count instead of storing a
        // rounded average that would lose precision after every update.
        return count == 0 ? 0.0 : sum / count;
    }
}
