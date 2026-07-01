package fhv.streamprocessing.pattern6.temperatureranking;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Mutable Kafka Streams state used to incrementally calculate temperature ranking.
 */
public class TemperatureRankingAggregate {
    private long windowStartEpochMs;
    private long windowEndEpochMs;
    private String rankingType = "HOTTEST_AND_COLDEST_BY_PEAK_TEMPERATURE";
    private List<RankedStation> hottest = new ArrayList<>();
    private List<RankedStation> coldest = new ArrayList<>();
    private Map<String, RankedStation> stationsById = new LinkedHashMap<>();

    public TemperatureRankingAggregate() {
    }

    public TemperatureRankingAggregate update(StationWindowTemperatureStats stats) {
        if (stats == null || stats.getStationId() == null) {
            return this;
        }

        windowStartEpochMs = stats.getWindowStartEpochMs();
        windowEndEpochMs = stats.getWindowEndEpochMs();
        rankingType = "HOTTEST_AND_COLDEST_BY_PEAK_TEMPERATURE";

        // Replace this station's previous summary: KTable updates may deliver
        // newer statistics for the same station many times during the replay.
        stationsById.put(
            stats.getStationId(),
            new RankedStation(
                stats.getStationId(),
                stats.getCount(),
                stats.getAverageTemperatureCelsius(),
                stats.getMinTemperatureCelsius(),
                stats.getMaxTemperatureCelsius()
            )
        );

        // Re-sort after each update and retain only the ten extreme stations.
        hottest = rankedStations(Comparator
            .comparing(RankedStation::maxTemperatureCelsius, Comparator.reverseOrder())
            .thenComparing(RankedStation::stationId));
        coldest = rankedStations(Comparator
            .comparing(RankedStation::minTemperatureCelsius)
            .thenComparing(RankedStation::stationId));
        return this;
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

    public String getRankingType() {
        return rankingType;
    }

    public void setRankingType(String rankingType) {
        this.rankingType = rankingType;
    }

    public List<RankedStation> getHottest() {
        return hottest;
    }

    public void setHottest(List<RankedStation> hottest) {
        this.hottest = hottest == null ? new ArrayList<>() : new ArrayList<>(hottest);
    }

    public List<RankedStation> getColdest() {
        return coldest;
    }

    public void setColdest(List<RankedStation> coldest) {
        this.coldest = coldest == null ? new ArrayList<>() : new ArrayList<>(coldest);
    }

    public Map<String, RankedStation> getStationsById() {
        return stationsById;
    }

    public void setStationsById(Map<String, RankedStation> stationsById) {
        this.stationsById = stationsById == null ? new LinkedHashMap<>() : new LinkedHashMap<>(stationsById);
    }

    public List<RankedStation> hottestStations(int limit) {
        return hottest.size() <= limit ? hottest : hottest.subList(0, limit);
    }

    public List<RankedStation> coldestStations(int limit) {
        return coldest.size() <= limit ? coldest : coldest.subList(0, limit);
    }

    private List<RankedStation> rankedStations(Comparator<RankedStation> comparator) {
        List<RankedStation> stations = new ArrayList<>(stationsById.values());
        stations.sort(comparator);
        return stations.size() <= 10 ? stations : new ArrayList<>(stations.subList(0, 10));
    }

    public record RankedStation(
        String stationId,
        long count,
        double averageTemperatureCelsius,
        double minTemperatureCelsius,
        double maxTemperatureCelsius
    ) {
    }
}
