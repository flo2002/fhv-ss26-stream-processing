package fhv.streamprocessing.pattern11.weatherregime;

/**
 * Feature vector and normalization rules used by the online weather-regime model.
 */
public record WeatherRegimeFeatureVector(
    double avgTemperatureCelsius,
    double temperatureRangeCelsius,
    double avgWindSpeedMetersPerSecond,
    double maxWindSpeedMetersPerSecond,
    double skyClarityScore,
    double rainDurationHours
) {
    public double[] normalized() {
        return new double[] {
            normalize(avgTemperatureCelsius, -30.0, 45.0),
            normalize(temperatureRangeCelsius, 0.0, 35.0),
            normalize(avgWindSpeedMetersPerSecond, 0.0, 25.0),
            normalize(maxWindSpeedMetersPerSecond, 0.0, 40.0),
            normalize(skyClarityScore, 0.0, 100.0),
            normalize(rainDurationHours, 0.0, 24.0)
        };
    }

    private static double normalize(double value, double min, double max) {
        double normalized = (value - min) / (max - min);
        return Math.max(0.0, Math.min(1.0, normalized));
    }
}
