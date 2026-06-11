package fhv.streamprocessing.pattern3.rapidchange;

import fhv.streamprocessing.model.NoaaObservation;
import java.time.Duration;
import java.time.OffsetDateTime;

public class RapidTemperatureChangeAggregate {
    private Double lastTemperature;
    private OffsetDateTime lastObservedAt;
    
    private double minRateOfChange = 0.0;
    private double maxRateOfChange = 0.0;
    private double sumRateOfChange = 0.0;
    private long rateOfChangeCount = 0;

    public RapidTemperatureChangeAggregate() {
    }

    public RapidTemperatureChangeAggregate add(NoaaObservation observation) {
        if (observation.temperatureCelsius() == null || observation.observedAt() == null) {
            return this;
        }

        if (lastObservedAt != null && lastTemperature != null) {
            double deltaTemp = observation.temperatureCelsius() - lastTemperature;
            Duration deltaTime = Duration.between(lastObservedAt, observation.observedAt());
            
            if (!deltaTime.isZero() && !deltaTime.isNegative()) {
                double hours = deltaTime.toMillis() / 3600000.0;
                double rate = deltaTemp / hours;

                if (rateOfChangeCount == 0) {
                    minRateOfChange = rate;
                    maxRateOfChange = rate;
                } else {
                    minRateOfChange = Math.min(minRateOfChange, rate);
                    maxRateOfChange = Math.max(maxRateOfChange, rate);
                }
                sumRateOfChange += rate;
                rateOfChangeCount++;
            }
        }

        lastTemperature = observation.temperatureCelsius();
        lastObservedAt = observation.observedAt();
        
        return this;
    }

    public Double getLastTemperature() {
        return lastTemperature;
    }

    public void setLastTemperature(Double lastTemperature) {
        this.lastTemperature = lastTemperature;
    }

    public OffsetDateTime getLastObservedAt() {
        return lastObservedAt;
    }

    public void setLastObservedAt(OffsetDateTime lastObservedAt) {
        this.lastObservedAt = lastObservedAt;
    }

    public double getMinRateOfChange() {
        return minRateOfChange;
    }

    public void setMinRateOfChange(double minRateOfChange) {
        this.minRateOfChange = minRateOfChange;
    }

    public double getMaxRateOfChange() {
        return maxRateOfChange;
    }

    public void setMaxRateOfChange(double maxRateOfChange) {
        this.maxRateOfChange = maxRateOfChange;
    }

    public double getSumRateOfChange() {
        return sumRateOfChange;
    }

    public void setSumRateOfChange(double sumRateOfChange) {
        this.sumRateOfChange = sumRateOfChange;
    }

    public long getRateOfChangeCount() {
        return rateOfChangeCount;
    }

    public void setRateOfChangeCount(long rateOfChangeCount) {
        this.rateOfChangeCount = rateOfChangeCount;
    }

    public double getAverageRateOfChange() {
        return rateOfChangeCount == 0 ? 0.0 : sumRateOfChange / rateOfChangeCount;
    }
}
