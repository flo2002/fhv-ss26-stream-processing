package fhv.streamprocessing.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.OffsetDateTime;
import org.junit.jupiter.api.Test;

class IsdRecordParserTest {
    private static final String SAMPLE_RECORD = "0104010010999992025032711004+70939-008669FM-12+001099999V0200051N0104199999999999999999-00651-00941100111ADDAA199999999KA1120M-00631KA2120N-00681MA1999999099991MD1310031+9999OC101321OD199901121999REMSYN004BUFR";

    @Test
    void parsesStationTimestampAndTemperature() {
        NoaaObservation observation = IsdRecordParser.parse(SAMPLE_RECORD, null, "/pub/data/noaa/2025/010010-99999-2025.gz", 751);

        assertEquals("010010-99999", observation.stationId());
        assertEquals(OffsetDateTime.parse("2025-03-27T11:00:00Z"), observation.observedAt());
        assertEquals(-6.5, observation.temperatureCelsius());
        assertEquals("1", observation.temperatureQualityCode());
        assertEquals("010010-99999|2025-03-27", observation.stationDayKey());
    }
}
