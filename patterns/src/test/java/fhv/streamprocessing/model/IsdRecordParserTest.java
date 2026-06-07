package fhv.streamprocessing.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import fhv.streamprocessing.pattern1.temperature.StationDayKey;
import fhv.streamprocessing.pattern5.rainduration.StationYearKey;
import java.time.OffsetDateTime;
import org.junit.jupiter.api.Test;

class IsdRecordParserTest {
    private static final String SAMPLE_RECORD = "0104010010999992025032711004+70939-008669FM-12+001099999V0200051N0104199999999999999999-00651-00941100111ADDAA199999999KA1120M-00631KA2120N-00681MA1999999099991MD1310031+9999OC101321OD199901121999REMSYN004BUFR";
    private static final String RAIN_RECORD = SAMPLE_RECORD.replace("AA199999999", "AA106001231");
    private static final String SECOND_RAIN_GROUP_RECORD = SAMPLE_RECORD.replace("AA199999999", "AA199999999AA212001231");
    private static final String CLEAR_VISIBILITY_RECORD = replaceRange(SAMPLE_RECORD, 78, 85, "0100001");

    @Test
    void parsesStationTimestampAndTemperature() {
        NoaaObservation observation = IsdRecordParser.parse(SAMPLE_RECORD, null, "/pub/data/noaa/2025/010010-99999-2025.gz", 751);

        assertEquals("010010-99999", observation.stationId());
        assertEquals(OffsetDateTime.parse("2025-03-27T11:00:00Z"), observation.observedAt());
        assertEquals(-6.5, observation.temperatureCelsius());
        assertEquals("1", observation.temperatureQualityCode());
        assertEquals(10.4, observation.windSpeedMetersPerSecond());
        assertEquals("1", observation.windSpeedQualityCode());
        assertEquals(null, observation.skyClarityScore());
        assertEquals("010010-99999|2025-03-27", StationDayKey.fromObservation(observation).asKey());
    }

    @Test
    void parsesSkyClarityScoreFromVisibilityDistance() {
        NoaaObservation observation = IsdRecordParser.parse(CLEAR_VISIBILITY_RECORD, null, "/pub/data/noaa/2025/010010-99999-2025.gz", 754);

        assertEquals(100.0, observation.skyClarityScore());
        assertEquals("1", observation.visibilityQualityCode());
    }

    @Test
    void parsesRainDurationFromLiquidPrecipitationGroup() {
        NoaaObservation observation = IsdRecordParser.parse(RAIN_RECORD, null, "/pub/data/noaa/2025/010010-99999-2025.gz", 752);

        assertEquals(6, observation.rainDurationHours());
        assertEquals("010010-99999|2025", StationYearKey.fromObservation(observation).asKey());
    }

    @Test
    void parsesRainDurationFromLaterLiquidPrecipitationGroup() {
        NoaaObservation observation = IsdRecordParser.parse(SECOND_RAIN_GROUP_RECORD, null, "/pub/data/noaa/2025/010010-99999-2025.gz", 753);

        assertEquals(12, observation.rainDurationHours());
    }

    private static String replaceRange(String value, int beginIndex, int endIndex, String replacement) {
        return value.substring(0, beginIndex) + replacement + value.substring(endIndex);
    }
}
