package fhv.streamprocessing.pattern11.weatherregime;

import fhv.streamprocessing.model.NoaaObservation;

public class StationDayWeatherAggregate {
    private double temperatureSum;
    private double minTemperatureCelsius = Double.POSITIVE_INFINITY;
    private double maxTemperatureCelsius = Double.NEGATIVE_INFINITY;
    private long temperatureCount;
    private double windSpeedSum;
    private double maxWindSpeedMetersPerSecond;
    private long windSpeedCount;
    private double skyClaritySum;
    private long skyClarityCount;
    private long precipitationObservationCount;
    private long rainDurationHours;
    private long observationCount;

    public StationDayWeatherAggregate add(NoaaObservation observation) {
        observationCount++;
        if (hasUsableTemperature(observation)) {
            temperatureSum += observation.temperatureCelsius();
            minTemperatureCelsius = Math.min(minTemperatureCelsius, observation.temperatureCelsius());
            maxTemperatureCelsius = Math.max(maxTemperatureCelsius, observation.temperatureCelsius());
            temperatureCount++;
        }
        if (hasUsableWindSpeed(observation)) {
            windSpeedSum += observation.windSpeedMetersPerSecond();
            maxWindSpeedMetersPerSecond = Math.max(maxWindSpeedMetersPerSecond, observation.windSpeedMetersPerSecond());
            windSpeedCount++;
        }
        if (hasUsableSkyClarity(observation)) {
            skyClaritySum += observation.skyClarityScore();
            skyClarityCount++;
        }
        if (observation.rainDurationHours() != null && observation.rainDurationHours() > 0) {
            precipitationObservationCount++;
            rainDurationHours += observation.rainDurationHours();
        }
        return this;
    }

    public double avgTemperatureCelsius() {
        return temperatureCount == 0 ? 0.0 : round1(temperatureSum / temperatureCount);
    }

    public double minTemperatureCelsius() {
        return temperatureCount == 0 ? 0.0 : round1(minTemperatureCelsius);
    }

    public double maxTemperatureCelsius() {
        return temperatureCount == 0 ? 0.0 : round1(maxTemperatureCelsius);
    }

    public double temperatureRangeCelsius() {
        return temperatureCount == 0 ? 0.0 : round1(maxTemperatureCelsius - minTemperatureCelsius);
    }

    public double avgWindSpeedMetersPerSecond() {
        return windSpeedCount == 0 ? 0.0 : round1(windSpeedSum / windSpeedCount);
    }

    public double maxWindSpeedMetersPerSecond() {
        return round1(maxWindSpeedMetersPerSecond);
    }

    public double skyClarityScore() {
        return skyClarityCount == 0 ? 0.0 : round1(skyClaritySum / skyClarityCount);
    }

    public long precipitationObservationCount() {
        return precipitationObservationCount;
    }

    public long rainDurationHours() {
        return rainDurationHours;
    }

    public long observationCount() {
        return observationCount;
    }

    public boolean hasEnoughSignal() {
        return temperatureCount > 0 || windSpeedCount > 0 || skyClarityCount > 0 || precipitationObservationCount > 0;
    }

    public WeatherRegimeFeatureVector toFeatureVector() {
        return new WeatherRegimeFeatureVector(
            avgTemperatureCelsius(),
            temperatureRangeCelsius(),
            avgWindSpeedMetersPerSecond(),
            maxWindSpeedMetersPerSecond(),
            skyClarityScore(),
            rainDurationHours()
        );
    }

    private static boolean hasUsableTemperature(NoaaObservation observation) {
        return observation.temperatureCelsius() != null
            && observation.temperatureQualityCode() != null
            && !observation.temperatureQualityCode().equals("9");
    }

    private static boolean hasUsableWindSpeed(NoaaObservation observation) {
        return observation.windSpeedMetersPerSecond() != null
            && observation.windSpeedQualityCode() != null
            && !observation.windSpeedQualityCode().equals("9");
    }

    private static boolean hasUsableSkyClarity(NoaaObservation observation) {
        return observation.skyClarityScore() != null
            && observation.visibilityQualityCode() != null
            && !observation.visibilityQualityCode().equals("9");
    }

    private static double round1(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

    public double getTemperatureSum() {
        return temperatureSum;
    }

    public void setTemperatureSum(double temperatureSum) {
        this.temperatureSum = temperatureSum;
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

    public long getTemperatureCount() {
        return temperatureCount;
    }

    public void setTemperatureCount(long temperatureCount) {
        this.temperatureCount = temperatureCount;
    }

    public double getWindSpeedSum() {
        return windSpeedSum;
    }

    public void setWindSpeedSum(double windSpeedSum) {
        this.windSpeedSum = windSpeedSum;
    }

    public double getMaxWindSpeedMetersPerSecond() {
        return maxWindSpeedMetersPerSecond;
    }

    public void setMaxWindSpeedMetersPerSecond(double maxWindSpeedMetersPerSecond) {
        this.maxWindSpeedMetersPerSecond = maxWindSpeedMetersPerSecond;
    }

    public long getWindSpeedCount() {
        return windSpeedCount;
    }

    public void setWindSpeedCount(long windSpeedCount) {
        this.windSpeedCount = windSpeedCount;
    }

    public double getSkyClaritySum() {
        return skyClaritySum;
    }

    public void setSkyClaritySum(double skyClaritySum) {
        this.skyClaritySum = skyClaritySum;
    }

    public long getSkyClarityCount() {
        return skyClarityCount;
    }

    public void setSkyClarityCount(long skyClarityCount) {
        this.skyClarityCount = skyClarityCount;
    }

    public long getPrecipitationObservationCount() {
        return precipitationObservationCount;
    }

    public void setPrecipitationObservationCount(long precipitationObservationCount) {
        this.precipitationObservationCount = precipitationObservationCount;
    }

    public long getRainDurationHours() {
        return rainDurationHours;
    }

    public void setRainDurationHours(long rainDurationHours) {
        this.rainDurationHours = rainDurationHours;
    }

    public long getObservationCount() {
        return observationCount;
    }

    public void setObservationCount(long observationCount) {
        this.observationCount = observationCount;
    }
}
