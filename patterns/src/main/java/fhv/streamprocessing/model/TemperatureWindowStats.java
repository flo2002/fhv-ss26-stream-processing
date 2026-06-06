package fhv.streamprocessing.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class TemperatureWindowStats {
    private long count;
    private double sumTemperatureCelsius;
    private double minTemperatureCelsius = Double.POSITIVE_INFINITY;
    private double maxTemperatureCelsius = Double.NEGATIVE_INFINITY;

    public TemperatureWindowStats() {
    }

    public TemperatureWindowStats add(NoaaObservation observation) {
        if (observation == null || observation.temperatureCelsius() == null) {
            return this;
        }

        double temperature = observation.temperatureCelsius();
        count++;
        sumTemperatureCelsius += temperature;
        minTemperatureCelsius = Math.min(minTemperatureCelsius, temperature);
        maxTemperatureCelsius = Math.max(maxTemperatureCelsius, temperature);
        return this;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public double getSumTemperatureCelsius() {
        return sumTemperatureCelsius;
    }

    public void setSumTemperatureCelsius(double sumTemperatureCelsius) {
        this.sumTemperatureCelsius = sumTemperatureCelsius;
    }

    public double getMinTemperatureCelsius() {
        return minTemperatureCelsius;
    }

    public void setMinTemperatureCelsius(double minTemperatureCelsius) {
        this.minTemperatureCelsius = minTemperatureCelsius;
    }

    public double getMaxTemperatureCelsius() {
        return maxTemperatureCelsius;
    }

    public void setMaxTemperatureCelsius(double maxTemperatureCelsius) {
        this.maxTemperatureCelsius = maxTemperatureCelsius;
    }

    @JsonIgnore
    public Double averageTemperatureCelsius() {
        return count == 0 ? null : sumTemperatureCelsius / count;
    }
}
