package fhv.streamprocessing.pattern6.temperatureranking;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Maintains incrementally sorted Top-10 lists for the hottest and coldest
 * stations. Only the ranking candidates are stored in Kafka state.
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

        RankedStation updatedStation = new RankedStation(
            stats.getStationId(),
            stats.getCount(),
            stats.getAverageTemperatureCelsius(),
            stats.getMinTemperatureCelsius(),
            stats.getMaxTemperatureCelsius()
        );

        // The station may already be in a Top-10 list. Remove its stale value,
        // insert the replacement at the sorted position, and trim to ten.
        // This is exact because a station maximum only rises and a minimum only
        // falls; an evicted station can re-enter only after another update.
        updateTopTen(hottest, updatedStation, hottestComparator());
        updateTopTen(coldest, updatedStation, coldestComparator());
        rebuildCandidateIndex();
        return this;
    }

    private void updateTopTen(
        List<RankedStation> ranking,
        RankedStation updatedStation,
        Comparator<RankedStation> comparator
    ) {
        ranking.removeIf(station -> station.stationId().equals(updatedStation.stationId()));

        int insertionPoint = Collections.binarySearch(ranking, updatedStation, comparator);
        if (insertionPoint < 0) {
            insertionPoint = -insertionPoint - 1;
        }
        ranking.add(insertionPoint, updatedStation);

        if (ranking.size() > 10) {
            ranking.remove(10);
        }
    }

    private void rebuildCandidateIndex() {
        stationsById = new LinkedHashMap<>();
        hottest.forEach(station -> stationsById.put(station.stationId(), station));
        coldest.forEach(station -> stationsById.put(station.stationId(), station));
    }

    private static Comparator<RankedStation> hottestComparator() {
        return Comparator
            .comparing(RankedStation::maxTemperatureCelsius, Comparator.reverseOrder())
            .thenComparing(RankedStation::stationId);
    }

    private static Comparator<RankedStation> coldestComparator() {
        return Comparator
            .comparing(RankedStation::minTemperatureCelsius)
            .thenComparing(RankedStation::stationId);
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

    public record RankedStation(
        String stationId,
        long count,
        double averageTemperatureCelsius,
        double minTemperatureCelsius,
        double maxTemperatureCelsius
    ) {
    }
}
