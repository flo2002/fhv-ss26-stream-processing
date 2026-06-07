package fhv.streamprocessing.pattern4.tourism;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class StationRegionMetadataLoaderTest {
    @Test
    void loadsCountryAndStateRegionsFromIsdHistoryRows() {
        String metadata = """
            USAF   WBAN  STATION NAME                  CTRY ST CALL  LAT     LON      ELEV(M) BEGIN    END
            """ + row("010010", "99999", "JAN MAYEN", "NO", null)
            + row("722950", "23174", "LOS ANGELES INTL", "US", "CA");

        StationRegionResolver resolver = StationRegionMetadataLoader.load(
            new ByteArrayInputStream(metadata.getBytes(StandardCharsets.UTF_8))
        );

        assertEquals(new WeatherRegion("NO", "Norway"), resolver.resolve("010010-99999"));
        assertEquals(new WeatherRegion("US-CA", "United States / CA"), resolver.resolve("722950-23174"));
        assertEquals(new WeatherRegion("unknown", "Unknown region"), resolver.resolve("999999-99999"));
    }

    private static String row(String usaf, String wban, String stationName, String countryCode, String stateCode) {
        return String.format(
            "%-6s %-5s %-30s%-5s%-3s%-6s%8s%9s%8s %-8s %-8s%n",
            usaf,
            wban,
            stationName,
            countryCode,
            stateCode == null ? "" : stateCode,
            "",
            "",
            "",
            "",
            "",
            ""
        );
    }
}
