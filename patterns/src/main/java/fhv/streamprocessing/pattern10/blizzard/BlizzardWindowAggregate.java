package fhv.streamprocessing.pattern10.blizzard;

import fhv.streamprocessing.model.NoaaObservation;

/**
 * Collects freezing, wind, and precipitation evidence for one blizzard detection window.
 */
public class BlizzardWindowAggregate {
    private long sampleCount;
    private long freezingObservationCount;
    private long strongWindObservationCount;
    private long precipitationObservationCount;
    private double minTemperatureCelsius = Double.POSITIVE_INFINITY;
    private double maxWindSpeedMetersPerSecond = Double.NEGATIVE_INFINITY;

    public BlizzardWindowAggregate() {
    }

    public BlizzardWindowAggregate add(
        NoaaObservation observation,
        double freezingThresholdCelsius,
        double strongWindThresholdMetersPerSecond
    ) {
        // One aggregate represents all evidence seen for one station/window.
        sampleCount++;

        if (observation.temperatureCelsius() != null && observation.temperatureQualityCode() != null && !observation.temperatureQualityCode().equals("9")) {
            minTemperatureCelsius = Math.min(minTemperatureCelsius, observation.temperatureCelsius());
            if (observation.temperatureCelsius() <= freezingThresholdCelsius) {
                // Counts, rather than one boolean, make the emitted event
                // explain how much evidence supported the detection.
                freezingObservationCount++;
            }
        }

        if (observation.windSpeedMetersPerSecond() != null && observation.windSpeedQualityCode() != null && !observation.windSpeedQualityCode().equals("9")) {
            maxWindSpeedMetersPerSecond = Math.max(maxWindSpeedMetersPerSecond, observation.windSpeedMetersPerSecond());
            if (observation.windSpeedMetersPerSecond() >= strongWindThresholdMetersPerSecond) {
                strongWindObservationCount++;
            }
        }

        if (observation.rainDurationHours() != null && observation.rainDurationHours() > 0) {
            precipitationObservationCount++;
        }

        return this;
    }

    public long getSampleCount() {
        return sampleCount;
    }

    public void setSampleCount(long sampleCount) {
        this.sampleCount = sampleCount;
    }

    public long getFreezingObservationCount() {
        return freezingObservationCount;
    }

    public void setFreezingObservationCount(long freezingObservationCount) {
        this.freezingObservationCount = freezingObservationCount;
    }

    public long getStrongWindObservationCount() {
        return strongWindObservationCount;
    }

    public void setStrongWindObservationCount(long strongWindObservationCount) {
        this.strongWindObservationCount = strongWindObservationCount;
    }

    public long getPrecipitationObservationCount() {
        return precipitationObservationCount;
    }

    public void setPrecipitationObservationCount(long precipitationObservationCount) {
        this.precipitationObservationCount = precipitationObservationCount;
    }

    public double getMinTemperatureCelsius() {
        return Double.isInfinite(minTemperatureCelsius) ? 0.0 : minTemperatureCelsius;
    }

    public void setMinTemperatureCelsius(double minTemperatureCelsius) {
        this.minTemperatureCelsius = minTemperatureCelsius;
    }

    public double getMaxWindSpeedMetersPerSecond() {
        return Double.isInfinite(maxWindSpeedMetersPerSecond) ? 0.0 : maxWindSpeedMetersPerSecond;
    }

    public void setMaxWindSpeedMetersPerSecond(double maxWindSpeedMetersPerSecond) {
        this.maxWindSpeedMetersPerSecond = maxWindSpeedMetersPerSecond;
    }
}
