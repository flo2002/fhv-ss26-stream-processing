package fhv.streamprocessing.pattern8.maritime;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;

public final class MaritimeRoutingTopology {
    private MaritimeRoutingTopology() {
    }

    public static KStream<String, RouteRecommendationEvent> build(
        KStream<String, AisPositionEvent> aisPositions,
        KStream<String, BuoyObservationEvent> buoyObservations,
        int routingYear,
        int joinWindowMinutes
    ) {
        KStream<String, AisPositionEvent> keyedAis = aisPositions
            .filter((key, event) -> event != null && event.observedAt() != null && event.seaAreaId() != null)
            .filter((key, event) -> event.observedAt().atZone(ZoneOffset.UTC).getYear() == routingYear)
            .selectKey((key, event) -> event.seaAreaId());

        KStream<String, BuoyObservationEvent> keyedBuoys = buoyObservations
            .filter((key, event) -> event != null && event.observedAt() != null && event.seaAreaId() != null)
            .filter((key, event) -> event.observedAt().atZone(ZoneOffset.UTC).getYear() == routingYear)
            .selectKey((key, event) -> event.seaAreaId());

        Duration joinWindow = Duration.ofMinutes(joinWindowMinutes);
        return keyedAis.join(
            keyedBuoys,
            (ais, buoy) -> recommendation(ais, buoy, joinWindow),
            JoinWindows.ofTimeDifferenceWithNoGrace(joinWindow),
            StreamJoined.with(
                org.apache.kafka.common.serialization.Serdes.String(),
                new fhv.streamprocessing.serde.JsonSerde<>(AisPositionEvent.class),
                new fhv.streamprocessing.serde.JsonSerde<>(BuoyObservationEvent.class)
            )
        ).selectKey((seaAreaId, event) -> event.mmsi() + "-" + event.observedAt());
    }

    private static RouteRecommendationEvent recommendation(
        AisPositionEvent ais,
        BuoyObservationEvent buoy,
        Duration joinWindow
    ) {
        double riskScore = Math.min(100.0, (buoy.waveHeightMeters() * 8.0) + (buoy.windSpeedMetersPerSecond() * 2.5));
        String riskClass = riskClass(riskScore);
        long etaDelayMinutes = etaDelayMinutes(riskClass);
        Instant updatedEta = ais.reportedEta() == null ? null : ais.reportedEta().plus(Duration.ofMinutes(etaDelayMinutes));

        return new RouteRecommendationEvent(
            ais.mmsi(),
            ais.vesselName(),
            ais.seaAreaId(),
            ais.observedAt().minus(joinWindow),
            ais.observedAt().plus(joinWindow),
            ais.observedAt(),
            ais.latitude(),
            ais.longitude(),
            ais.speedOverGroundKnots(),
            ais.courseOverGroundDegrees(),
            buoy.waveHeightMeters(),
            buoy.windSpeedMetersPerSecond(),
            riskScore,
            riskClass,
            recommendationText(riskClass, ais.seaAreaId()),
            ais.reportedEta(),
            updatedEta,
            etaDelayMinutes
        );
    }

    private static String riskClass(double riskScore) {
        if (riskScore >= 75.0) {
            return "HIGH";
        }
        if (riskScore >= 45.0) {
            return "MODERATE";
        }
        return "LOW";
    }

    private static long etaDelayMinutes(String riskClass) {
        return switch (riskClass) {
            case "HIGH" -> 90;
            case "MODERATE" -> 30;
            default -> 0;
        };
    }

    private static String recommendationText(String riskClass, String seaAreaId) {
        return switch (riskClass) {
            case "HIGH" -> "Reroute around " + seaAreaId + " or reduce speed until wave and wind conditions improve.";
            case "MODERATE" -> "Reduce speed through " + seaAreaId + " and monitor the next buoy update.";
            default -> "Continue planned route through " + seaAreaId + ".";
        };
    }
}
