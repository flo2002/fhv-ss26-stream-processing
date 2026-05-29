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
            return new NoaaObservation(null, null, fallbackObservedAt, null, null, sourcePath, recordNumber, null);
        }

        OffsetDateTime observedAt = parseObservedAt(record, fallbackObservedAt);
        return new NoaaObservation(
            parseStationId(record),
            observedAt == null ? null : observedAt.toLocalDate(),
            observedAt,
            parseAirTemperatureCelsius(record),
            substringOrNull(record, 92, 93),
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

    private static String substringOrNull(String value, int beginIndex, int endIndex) {
        if (value.length() < endIndex) {
            return null;
        }
        return value.substring(beginIndex, endIndex);
    }
}
