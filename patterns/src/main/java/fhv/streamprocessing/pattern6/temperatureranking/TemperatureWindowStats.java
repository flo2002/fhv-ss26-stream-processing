package fhv.streamprocessing.pattern6.temperatureranking;

import fhv.streamprocessing.model.NoaaObservation;

public class TemperatureWindowStats {
    private long count;
    private double sum;
    private double minTemperatureCelsius = Double.POSITIVE_INFINITY;
    private double maxTemperatureCelsius = Double.NEGATIVE_INFINITY;

    public TemperatureWindowStats() {
    }

    public TemperatureWindowStats add(NoaaObservation observation) {
        double temperature = observation.temperatureCelsius();
        count++;
        sum += temperature;
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

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public double getMinTemperatureCelsius() {
        return count == 0 ? 0.0 : minTemperatureCelsius;
    }

    public void setMinTemperatureCelsius(double minTemperatureCelsius) {
        this.minTemperatureCelsius = minTemperatureCelsius;
    }

    public double getMaxTemperatureCelsius() {
        return count == 0 ? 0.0 : maxTemperatureCelsius;
    }

    public void setMaxTemperatureCelsius(double maxTemperatureCelsius) {
        this.maxTemperatureCelsius = maxTemperatureCelsius;
    }

    public double averageTemperatureCelsius() {
        return count == 0 ? 0.0 : sum / count;
    }
}
