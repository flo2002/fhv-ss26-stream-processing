package fhv.streamprocessing.pattern4.tourism;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class StationRegionMetadataLoaderTest {
    @Test
    void loadsCountryAndStateRegionsFromIsdHistoryRows() {
        String metadata = """
            Integrated Surface Database Station History, August 2025

            USAF = Air Force station ID. May contain a letter in the first position.
            CTRY = FIPS country ID
            - Missing station name, etc indicate the metadata are not currently available.
            The term "bogus" indicates that the station name, etc are not available.
            USAF   WBAN  STATION NAME                  CTRY ST CALL  LAT     LON      ELEV(M) BEGIN    END
            """ + row("010010", "99999", "JAN MAYEN", "NO", null)
            + row("010070", "99999", "NY-ALESUND", "SV", null)
            + row("722950", "23174", "LOS ANGELES INTL", "US", "CA");

        StationRegionResolver resolver = StationRegionMetadataLoader.load(
            new ByteArrayInputStream(metadata.getBytes(StandardCharsets.UTF_8))
        );

        assertEquals(new WeatherRegion("NO", "Norway"), resolver.resolve("010010-99999"));
        assertEquals(new WeatherRegion("SV", "Svalbard"), resolver.resolve("010070-99999"));
        assertEquals(new WeatherRegion("US-CA", "United States / CA"), resolver.resolve("722950-23174"));
        assertEquals(new WeatherRegion("unknown", "Unknown region"), resolver.resolve("999999-99999"));
        assertEquals(3, resolver.size());
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
