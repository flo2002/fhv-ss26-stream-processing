package fhv.streamprocessing.pattern7.forecasting;

import fhv.streamprocessing.model.NoaaObservation;
import java.io.Serializable;

public class TemperatureTrendAggregate implements Serializable {
    private long count;
    private double sumX;
    private double sumY;
    private double sumXY;
    private double sumX2;
    private double firstTimestamp;
    private double lastTimestamp;

    public TemperatureTrendAggregate add(NoaaObservation observation) {
        if (observation.temperatureCelsius() == null || observation.observedAt() == null) {
            return this;
        }

        double x = observation.observedAt().toEpochSecond();
        double y = observation.temperatureCelsius();

        if (count == 0) {
            firstTimestamp = x;
            lastTimestamp = x;
        } else {
            lastTimestamp = Math.max(lastTimestamp, x);
        }

        // Relative x to keep numbers manageable
        double relativeX = x - firstTimestamp;

        count++;
        sumX += relativeX;
        sumY += y;
        sumXY += relativeX * y;
        sumX2 += relativeX * relativeX;

        return this;
    }

    public double getSlope() {
        if (count < 2) {
            return 0.0;
        }
        double denominator = (count * sumX2 - sumX * sumX);
        if (denominator == 0) {
            return 0.0;
        }
        return (count * sumXY - sumX * sumY) / denominator;
    }

    public double getIntercept() {
        if (count == 0) {
            return 0.0;
        }
        return (sumY - getSlope() * sumX) / count;
    }

    public double getLatestAverage() {
        if (count == 0) {
            return 0.0;
        }
        return sumY / count;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public double getSumX() {
        return sumX;
    }

    public void setSumX(double sumX) {
        this.sumX = sumX;
    }

    public double getSumY() {
        return sumY;
    }

    public void setSumY(double sumY) {
        this.sumY = sumY;
    }

    public double getSumXY() {
        return sumXY;
    }

    public void setSumXY(double sumXY) {
        this.sumXY = sumXY;
    }

    public double getSumX2() {
        return sumX2;
    }

    public void setSumX2(double sumX2) {
        this.sumX2 = sumX2;
    }

    public double getFirstTimestamp() {
        return firstTimestamp;
    }

    public void setFirstTimestamp(double firstTimestamp) {
        this.firstTimestamp = firstTimestamp;
    }

    public double getLastTimestamp() {
        return lastTimestamp;
    }

    public void setLastTimestamp(double lastTimestamp) {
        this.lastTimestamp = lastTimestamp;
    }

    /** Latest observation time relative to the regression origin (firstTimestamp). */
    public double getLastRelativeX() {
        return lastTimestamp - firstTimestamp;
    }

    /** Fitted regression value at the latest observation, i.e. the "current" trend value. */
    public double getValueAtLastObservation() {
        return getIntercept() + getSlope() * getLastRelativeX();
    }
}
