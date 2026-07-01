package fhv.streamprocessing.pattern4.tourism;

import fhv.streamprocessing.model.NoaaObservation;

/**
 * Mutable Kafka Streams state used to incrementally calculate tourism weather quality.
 */
public class TourismWeatherQualityAggregate {
    private String regionId;
    private String regionName;
    private double temperatureSum;
    private double windSpeedSum;
    private double skyClaritySum;
    private long observationCount;

    public TourismWeatherQualityAggregate() {
    }

    public TourismWeatherQualityAggregate add(NoaaObservation observation, WeatherRegion region) {
        if (regionId == null) {
            regionId = region.id();
            regionName = region.name();
        }
        temperatureSum += observation.temperatureCelsius();
        windSpeedSum += observation.windSpeedMetersPerSecond();
        skyClaritySum += observation.skyClarityScore();
        observationCount++;
        return this;
    }

    public String regionId() {
        return regionId;
    }

    public String regionName() {
        return regionName;
    }

    public double avgTemperatureCelsius() {
        return observationCount == 0 ? 0.0 : round1(temperatureSum / observationCount);
    }

    public double avgWindSpeedMetersPerSecond() {
        return observationCount == 0 ? 0.0 : round1(windSpeedSum / observationCount);
    }

    public double avgSkyClarityScore() {
        return observationCount == 0 ? 0.0 : round1(skyClaritySum / observationCount);
    }

    public long observationCount() {
        return observationCount;
    }

    public double qualityScore() {
        if (observationCount == 0) {
            return 0.0;
        }
        // Convert the three differently scaled measurements to 0-100 sub-scores
        // before applying the documented tourism-comfort weights.
        double temperatureScore = Math.max(0.0, 100.0 - Math.abs(avgTemperatureCelsius() - 22.0) * 6.0);
        double windScore = Math.max(0.0, 100.0 - avgWindSpeedMetersPerSecond() * 3.5);
        return round1(temperatureScore * 0.55 + windScore * 0.25 + avgSkyClarityScore() * 0.20);
    }

    public String qualityClass() {
        double score = qualityScore();
        if (score >= 80.0) {
            return "excellent";
        }
        if (score >= 60.0) {
            return "good";
        }
        if (score >= 40.0) {
            return "ok";
        }
        return "poor";
    }

    private static double round1(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

    public String getRegionId() {
        return regionId;
    }

    public void setRegionId(String regionId) {
        this.regionId = regionId;
    }

    public String getRegionName() {
        return regionName;
    }

    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }

    public double getTemperatureSum() {
        return temperatureSum;
    }

    public void setTemperatureSum(double temperatureSum) {
        this.temperatureSum = temperatureSum;
    }

    public double getWindSpeedSum() {
        return windSpeedSum;
    }

    public void setWindSpeedSum(double windSpeedSum) {
        this.windSpeedSum = windSpeedSum;
    }

    public double getSkyClaritySum() {
        return skyClaritySum;
    }

    public void setSkyClaritySum(double skyClaritySum) {
        this.skyClaritySum = skyClaritySum;
    }

    public long getObservationCount() {
        return observationCount;
    }

    public void setObservationCount(long observationCount) {
        this.observationCount = observationCount;
    }
}
