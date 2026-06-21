import csv
import gzip
import io
import json
import logging
import os
import signal
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

import zstandard
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


LOGGER = logging.getLogger("marine_pattern8_producer")
STOP = False


AIS_BASE_URL = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2025"
NDBC_STDMET_BASE_URL = "https://www.ndbc.noaa.gov/data/historical/stdmet"


@dataclass(frozen=True)
class BuoyStation:
    station_id: str
    sea_area_id: str
    latitude: float
    longitude: float


SEA_AREAS: Tuple[Tuple[str, float, float, float, float], ...] = (
    ("BAHAMAS_NORTH", 25.0, 29.0, -74.0, -69.0),
    ("BAHAMAS_EAST", 22.5, 25.5, -70.5, -66.5),
    ("CARIB_PR_SOUTH", 17.5, 18.2, -67.6, -65.7),
    ("CARIB_PR_WEST", 17.8, 19.2, -68.4, -66.7),
    ("CARIB_PR_NORTH", 18.0, 21.5, -66.9, -64.4),
    ("CARIB_USVI", 17.2, 19.0, -65.4, -64.0),
    ("CARIB_EAST", 15.5, 17.5, -64.2, -62.5),
    ("CARIB_CENTRAL", 13.0, 17.5, -77.0, -70.0),
    ("CARIB_WEST", 16.0, 21.5, -86.5, -80.0),
)


DEFAULT_BUOY_STATIONS = (
    "41115:CARIB_PR_WEST:18.376:-67.280,"
    "41043:CARIB_PR_NORTH:21.090:-64.864,"
    "41052:CARIB_USVI:18.249:-64.763,"
    "41056:CARIB_USVI:18.261:-65.464,"
    "42085:CARIB_PR_SOUTH:17.870:-66.537,"
    "42060:CARIB_EAST:16.428:-63.210,"
    "41046:BAHAMAS_EAST:23.822:-68.384,"
    "41047:BAHAMAS_NORTH:27.514:-71.483,"
    "42058:CARIB_CENTRAL:14.923:-74.918,"
    "42056:CARIB_WEST:19.820:-84.945,"
    "42057:CARIB_WEST:16.908:-81.422"
)


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        raise ValueError(f"{name} must be an integer, got {value!r}") from None


def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        raise ValueError(f"{name} must be a number, got {value!r}") from None


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default

    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean, got {value!r}")


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def stop_handler(signum, _frame) -> None:
    global STOP
    LOGGER.info("received signal %s, stopping after current file", signum)
    STOP = True


def connect_producer(bootstrap_servers: str) -> KafkaProducer:
    for attempt in range(1, 31):
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                key_serializer=lambda value: value.encode("utf-8"),
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                linger_ms=10,
            )
        except NoBrokersAvailable:
            LOGGER.info("waiting for Kafka at %s, attempt %s/30", bootstrap_servers, attempt)
            time.sleep(2)
    raise NoBrokersAvailable(f"Could not connect to Kafka at {bootstrap_servers}")


def dates_between(start_date: date, end_date: date) -> Iterator[date]:
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def load_state(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {"completed_dates": []}
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_state(path: Path, state: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2, sort_keys=True)
    tmp_path.replace(path)


def mark_date_completed(path: Path, state: Dict[str, object], current_date: date) -> None:
    completed_dates = set(state.get("completed_dates", []))
    completed_dates.add(current_date.isoformat())
    state["completed_dates"] = sorted(completed_dates)
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    save_state(path, state)


def parse_buoy_stations(value: str) -> List[BuoyStation]:
    stations: List[BuoyStation] = []
    for item in value.split(","):
        if not item.strip():
            continue
        parts = item.split(":")
        if len(parts) != 4:
            raise ValueError(
                "NDBC_BUOY_STATIONS entries must be station:seaAreaId:latitude:longitude, "
                f"got {item!r}"
            )
        stations.append(BuoyStation(parts[0], parts[1], float(parts[2]), float(parts[3])))
    if not stations:
        raise ValueError("NDBC_BUOY_STATIONS must contain at least one station")
    return stations


def sea_area_for(latitude: float, longitude: float) -> Optional[str]:
    for sea_area_id, min_lat, max_lat, min_lon, max_lon in SEA_AREAS:
        if min_lat <= latitude <= max_lat and min_lon <= longitude <= max_lon:
            return sea_area_id
    return None


def open_url(url: str, timeout: int):
    request = urllib.request.Request(url, headers={"User-Agent": "fhv-stream-processing-pattern8/1.0"})
    return urllib.request.urlopen(request, timeout=timeout)


def parse_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped or stripped in {"MM", "M", "NA", "N/A"}:
        return None
    try:
        parsed = float(stripped)
    except ValueError:
        return None
    if parsed >= 99.0 and stripped.startswith("99"):
        return None
    if parsed >= 999.0:
        return None
    return parsed


def parse_instant(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            parsed = datetime.strptime(value.strip(), fmt).replace(tzinfo=timezone.utc)
            return parsed.isoformat().replace("+00:00", "Z")
        except ValueError:
            continue
    return None


def parse_ais_row(row: Dict[str, str]) -> Optional[Dict[str, object]]:
    lat = parse_float(row.get("LAT") or row.get("latitude"))
    lon = parse_float(row.get("LON") or row.get("longitude"))
    if lat is None or lon is None:
        return None

    sea_area_id = sea_area_for(lat, lon)
    if sea_area_id is None:
        return None

    observed_at = parse_instant(row.get("BaseDateTime") or row.get("basedatetime") or row.get("base_date_time"))
    mmsi = (row.get("MMSI") or row.get("mmsi") or "").strip()
    if not observed_at or not mmsi:
        return None

    return {
        "mmsi": mmsi,
        "vesselName": (row.get("VesselName") or row.get("vessel_name") or "").strip() or None,
        "observedAt": observed_at,
        "latitude": lat,
        "longitude": lon,
        "speedOverGroundKnots": parse_float(row.get("SOG") or row.get("sog")) or 0.0,
        "courseOverGroundDegrees": parse_float(row.get("COG") or row.get("cog")) or 0.0,
        "destination": (row.get("Destination") or row.get("destination") or "").strip() or None,
        "reportedEta": parse_instant(row.get("ETA") or row.get("eta")),
        "seaAreaId": sea_area_id,
    }


def read_ais_events(
    file_date: date,
    base_url: str,
    timeout: int,
    max_records: int,
) -> List[Dict[str, object]]:
    url = f"{base_url.rstrip('/')}/ais-{file_date.isoformat()}.csv.zst"
    events: List[Dict[str, object]] = []
    LOGGER.info("reading AIS file %s", url)
    try:
        with open_url(url, timeout) as response:
            decompressor = zstandard.ZstdDecompressor()
            with decompressor.stream_reader(response) as reader:
                text_reader = io.TextIOWrapper(reader, encoding="utf-8", newline="")
                for row in csv.DictReader(text_reader):
                    if STOP:
                        break
                    event = parse_ais_row(row)
                    if event is None:
                        continue
                    events.append(event)
                    if max_records > 0 and len(events) >= max_records:
                        break
    except urllib.error.HTTPError as exception:
        LOGGER.warning("could not read AIS file %s: HTTP %s", url, exception.code)
        return []

    events.sort(key=lambda event: str(event["observedAt"]))
    LOGGER.info("selected %s AIS records for %s", len(events), file_date)
    return events


def parse_ndbc_event(station: BuoyStation, columns: List[str], values: List[str]) -> Optional[Dict[str, object]]:
    row = dict(zip(columns, values))
    try:
        observed_at = datetime(
            int(row["#YY"]),
            int(row["MM"]),
            int(row["DD"]),
            int(row["hh"]),
            int(row.get("mm", "0")),
            tzinfo=timezone.utc,
        )
    except (KeyError, ValueError):
        return None

    wave_height = parse_float(row.get("WVHT"))
    wind_speed = parse_float(row.get("WSPD"))
    if wave_height is None or wind_speed is None:
        return None

    return {
        "buoyId": station.station_id,
        "observedAt": observed_at.isoformat().replace("+00:00", "Z"),
        "latitude": station.latitude,
        "longitude": station.longitude,
        "seaAreaId": station.sea_area_id,
        "waveHeightMeters": wave_height,
        "windSpeedMetersPerSecond": wind_speed,
        "windDirectionDegrees": parse_float(row.get("WDIR")) or 0.0,
    }


def read_buoy_events(
    station: BuoyStation,
    year: int,
    wanted_dates: set[date],
    base_url: str,
    timeout: int,
) -> List[Dict[str, object]]:
    url = f"{base_url.rstrip('/')}/{station.station_id}h{year}.txt.gz"
    events: List[Dict[str, object]] = []
    LOGGER.info("reading NDBC buoy file %s", url)
    try:
        with open_url(url, timeout) as response:
            with gzip.GzipFile(fileobj=response) as gzip_file:
                text_reader = io.TextIOWrapper(gzip_file, encoding="utf-8")
                columns: Optional[List[str]] = None
                for line in text_reader:
                    if STOP:
                        break
                    stripped = line.strip()
                    if not stripped:
                        continue
                    if stripped.startswith("#YY"):
                        columns = stripped.split()
                        continue
                    if stripped.startswith("#") or columns is None:
                        continue
                    values = stripped.split()
                    event = parse_ndbc_event(station, columns, values)
                    if event is None:
                        continue
                    observed_date = parse_date(str(event["observedAt"])[:10])
                    if observed_date in wanted_dates:
                        events.append(event)
    except urllib.error.HTTPError as exception:
        LOGGER.warning("could not read NDBC file %s: HTTP %s", url, exception.code)
        return []

    LOGGER.info("selected %s buoy records for station %s", len(events), station.station_id)
    return events


def kafka_timestamp_ms(event: Dict[str, object]) -> int:
    observed_at = str(event["observedAt"]).replace("Z", "+00:00")
    return int(datetime.fromisoformat(observed_at).astimezone(timezone.utc).timestamp() * 1000)


def emit_events(
    producer: KafkaProducer,
    ais_topic: str,
    buoy_topic: str,
    events: Iterable[Tuple[str, Dict[str, object]]],
    interval_seconds: float,
) -> int:
    sent = 0
    for event_type, event in events:
        if STOP:
            break
        if event_type == "ais":
            topic = ais_topic
            key = str(event["mmsi"])
        else:
            topic = buoy_topic
            key = str(event["buoyId"])
        producer.send(topic, key=key, value=event, timestamp_ms=kafka_timestamp_ms(event))
        sent += 1
        LOGGER.debug("sent %s key=%s observedAt=%s seaAreaId=%s", event_type, key, event["observedAt"], event["seaAreaId"])
        if interval_seconds > 0:
            time.sleep(interval_seconds)
    producer.flush()
    return sent


def merge_events(ais_events: List[Dict[str, object]], buoy_events: List[Dict[str, object]]) -> List[Tuple[str, Dict[str, object]]]:
    timeline = [("ais", event) for event in ais_events] + [("buoy", event) for event in buoy_events]
    timeline.sort(key=lambda item: str(item[1]["observedAt"]))
    return timeline


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    signal.signal(signal.SIGTERM, stop_handler)
    signal.signal(signal.SIGINT, stop_handler)

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19094")
    ais_topic = os.getenv("KAFKA_AIS_TOPIC", "marine.ais.positions")
    buoy_topic = os.getenv("KAFKA_BUOY_TOPIC", "marine.buoy.observations")
    year = env_int("MARITIME_DATA_YEAR", 2025)
    start_date = parse_date(os.getenv("MARITIME_START_DATE", f"{year}-06-01"))
    end_date = parse_date(os.getenv("MARITIME_END_DATE", start_date.isoformat()))
    ais_base_url = os.getenv("AIS_BASE_URL", AIS_BASE_URL)
    ndbc_base_url = os.getenv("NDBC_STDMET_BASE_URL", NDBC_STDMET_BASE_URL)
    buoy_stations = parse_buoy_stations(os.getenv("NDBC_BUOY_STATIONS", DEFAULT_BUOY_STATIONS))
    timeout_seconds = env_int("SOURCE_TIMEOUT_SECONDS", 600)
    max_ais_records_per_day = env_int("MAX_AIS_RECORDS_PER_DAY", 0)
    interval_seconds = env_float("EMIT_INTERVAL_SECONDS", 0.0)
    state_path = Path(os.getenv("STATE_FILE", "/app/state/marine_pattern8_state.json"))
    reset_state = env_bool("RESET_STATE", False)

    selected_dates = list(dates_between(start_date, end_date))
    wanted_dates = set(selected_dates)
    state = {"completed_dates": []} if reset_state else load_state(state_path)
    completed_dates = set(state.get("completed_dates", []))

    LOGGER.info(
        "starting real marine data producer year=%s start=%s end=%s ais_base=%s ndbc_base=%s stations=%s",
        year,
        start_date,
        end_date,
        ais_base_url,
        ndbc_base_url,
        ",".join(station.station_id for station in buoy_stations),
    )

    producer = connect_producer(bootstrap_servers)
    try:
        all_buoy_events: Dict[date, List[Dict[str, object]]] = {current_date: [] for current_date in selected_dates}
        for station in buoy_stations:
            for event in read_buoy_events(station, year, wanted_dates, ndbc_base_url, timeout_seconds):
                all_buoy_events[parse_date(str(event["observedAt"])[:10])].append(event)

        for current_date in selected_dates:
            if STOP:
                break
            if current_date.isoformat() in completed_dates:
                LOGGER.info("skipping completed marine date %s", current_date)
                continue

            ais_events = read_ais_events(current_date, ais_base_url, timeout_seconds, max_ais_records_per_day)
            buoy_events = sorted(all_buoy_events.get(current_date, []), key=lambda event: str(event["observedAt"]))
            sent = emit_events(
                producer,
                ais_topic,
                buoy_topic,
                merge_events(ais_events, buoy_events),
                interval_seconds,
            )
            LOGGER.info(
                "completed marine date %s sent=%s ais_records=%s buoy_records=%s",
                current_date,
                sent,
                len(ais_events),
                len(buoy_events),
            )
            mark_date_completed(state_path, state, current_date)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
