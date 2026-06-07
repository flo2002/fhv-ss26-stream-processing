package fhv.streamprocessing.model;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public final class IsdRecordParser {
    private static final DateTimeFormatter OBSERVED_AT_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHHmm");

    private IsdRecordParser() {
    }

    public static NoaaObservation parse(String record, OffsetDateTime fallbackObservedAt, String sourcePath, long recordNumber) {
        if (record == null) {
            return new NoaaObservation(null, null, fallbackObservedAt, null, null, null, null, null, null, null, sourcePath, recordNumber, null);
        }

        OffsetDateTime observedAt = parseObservedAt(record, fallbackObservedAt);
        return new NoaaObservation(
            parseStationId(record),
            observedAt == null ? null : observedAt.toLocalDate(),
            observedAt,
            parseAirTemperatureCelsius(record),
            substringOrNull(record, 92, 93),
            parseWindSpeedMetersPerSecond(record),
            substringOrNull(record, 69, 70),
            parseSkyClarityScore(record),
            substringOrNull(record, 84, 85),
            parseRainDurationHours(record),
            sourcePath,
            recordNumber,
            record
        );
    }

    private static String parseStationId(String record) {
        String usaf = substringOrNull(record, 4, 10);
        String wban = substringOrNull(record, 10, 15);
        if (usaf == null || wban == null) {
            return null;
        }
        return usaf + "-" + wban;
    }

    private static OffsetDateTime parseObservedAt(String record, OffsetDateTime fallbackObservedAt) {
        String date = substringOrNull(record, 15, 23);
        String time = substringOrNull(record, 23, 27);
        if (date == null || time == null) {
            return fallbackObservedAt;
        }

        try {
            return java.time.LocalDateTime.parse(date + time, OBSERVED_AT_FORMAT).atOffset(ZoneOffset.UTC);
        } catch (DateTimeParseException ignored) {
            return fallbackObservedAt;
        }
    }

    private static Double parseAirTemperatureCelsius(String record) {
        String rawTemperature = substringOrNull(record, 87, 92);
        if (rawTemperature == null || rawTemperature.equals("+9999") || rawTemperature.equals("-9999")) {
            return null;
        }

        try {
            return Integer.parseInt(rawTemperature) / 10.0;
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static Double parseWindSpeedMetersPerSecond(String record) {
        String rawWindSpeed = substringOrNull(record, 65, 69);
        String qualityCode = substringOrNull(record, 69, 70);
        if (rawWindSpeed == null || qualityCode == null || rawWindSpeed.equals("9999") || qualityCode.equals("9")) {
            return null;
        }

        try {
            return Integer.parseInt(rawWindSpeed) / 10.0;
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static Double parseSkyClarityScore(String record) {
        String rawVisibilityMeters = substringOrNull(record, 78, 84);
        String qualityCode = substringOrNull(record, 84, 85);
        if (rawVisibilityMeters == null
            || qualityCode == null
            || rawVisibilityMeters.equals("999999")
            || qualityCode.equals("9")) {
            return null;
        }

        try {
            int visibilityMeters = Integer.parseInt(rawVisibilityMeters);
            return round1(Math.min(100.0, Math.max(0.0, visibilityMeters / 10000.0 * 100.0)));
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static Integer parseRainDurationHours(String record) {
        Integer rainDurationHours = null;
        for (int precipitationGroup = 1; precipitationGroup <= 4; precipitationGroup++) {
            rainDurationHours = parseRainDurationHours(record, "AA" + precipitationGroup);
            if (rainDurationHours != null) {
                break;
            }
        }
        return rainDurationHours;
    }

    private static Integer parseRainDurationHours(String record, String precipitationGroup) {
        int precipitationIndex = record.indexOf(precipitationGroup);
        if (precipitationIndex < 0) {
            return null;
        }

        String periodQuantity = substringOrNull(record, precipitationIndex + 3, precipitationIndex + 5);
        String depthDimension = substringOrNull(record, precipitationIndex + 5, precipitationIndex + 9);
        String qualityCode = substringOrNull(record, precipitationIndex + 10, precipitationIndex + 11);
        if (periodQuantity == null
            || depthDimension == null
            || periodQuantity.equals("99")
            || depthDimension.equals("9999")
            || qualityCode == null
            || qualityCode.equals("9")) {
            return null;
        }

        try {
            int rainDepthTenthsMillimeter = Integer.parseInt(depthDimension);
            if (rainDepthTenthsMillimeter <= 0) {
                return null;
            }
            return Integer.parseInt(periodQuantity);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static String substringOrNull(String value, int beginIndex, int endIndex) {
        if (value.length() < endIndex) {
            return null;
        }
        return value.substring(beginIndex, endIndex);
    }

    private static double round1(double value) {
        return Math.round(value * 10.0) / 10.0;
    }
}
