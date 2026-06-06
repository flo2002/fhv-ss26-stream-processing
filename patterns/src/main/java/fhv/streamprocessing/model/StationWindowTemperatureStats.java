package fhv.streamprocessing.model;

public class StationWindowTemperatureStats {
    private String stationId;
    private long windowStartEpochMs;
    private long windowEndEpochMs;
    private long count;
    private double minTemperatureCelsius;
    private double maxTemperatureCelsius;
    private double averageTemperatureCelsius;

    public StationWindowTemperatureStats() {
    }

    public StationWindowTemperatureStats(
        String stationId,
        long windowStartEpochMs,
        long windowEndEpochMs,
        long count,
        double minTemperatureCelsius,
        double maxTemperatureCelsius,
        double averageTemperatureCelsius
    ) {
        this.stationId = stationId;
        this.windowStartEpochMs = windowStartEpochMs;
        this.windowEndEpochMs = windowEndEpochMs;
        this.count = count;
        this.minTemperatureCelsius = minTemperatureCelsius;
        this.maxTemperatureCelsius = maxTemperatureCelsius;
        this.averageTemperatureCelsius = averageTemperatureCelsius;
    }

    public String getStationId() {
        return stationId;
    }

    public void setStationId(String stationId) {
        this.stationId = stationId;
    }

    public long getWindowStartEpochMs() {
        return windowStartEpochMs;
    }

    public void setWindowStartEpochMs(long windowStartEpochMs) {
        this.windowStartEpochMs = windowStartEpochMs;
    }

    public long getWindowEndEpochMs() {
        return windowEndEpochMs;
    }

    public void setWindowEndEpochMs(long windowEndEpochMs) {
        this.windowEndEpochMs = windowEndEpochMs;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
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

    public double getAverageTemperatureCelsius() {
        return averageTemperatureCelsius;
    }

    public void setAverageTemperatureCelsius(double averageTemperatureCelsius) {
        this.averageTemperatureCelsius = averageTemperatureCelsius;
    }
}
