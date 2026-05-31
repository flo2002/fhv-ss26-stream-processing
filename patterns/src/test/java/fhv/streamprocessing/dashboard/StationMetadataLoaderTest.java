package fhv.streamprocessing.dashboard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class StationMetadataLoaderTest {
    @Test
    void parsesIsdStationHistoryRows() {
        String line = "010010 99999 JAN MAYEN                     NO      ENJA  +70.933  -008.667 +0009.0 19310101 20260530";

        var metadata = StationMetadataLoader.parseLine(line);

        assertTrue(metadata.isPresent());
        assertEquals("010010-99999", metadata.get().stationId());
        assertEquals("JAN MAYEN", metadata.get().stationLocation());
        assertEquals("NO", metadata.get().countryCode());
        assertEquals("ENJA", metadata.get().icaoCode());
        assertEquals(70.933, metadata.get().latitude());
        assertEquals(-8.667, metadata.get().longitude());
        assertEquals(9.0, metadata.get().elevationMeters());
        assertEquals("19310101", metadata.get().periodBegin());
        assertEquals("20260530", metadata.get().periodEnd());
    }
}
