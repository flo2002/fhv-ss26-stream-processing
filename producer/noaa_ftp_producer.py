import fnmatch
import ftplib
import gzip
import json
import logging
import os
import signal
import socket
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable


LOGGER = logging.getLogger("noaa_ftp_producer")
STOP = False


@dataclass
class RemoteFile:
    path: str
    name: str
    size: Optional[int]
    modified: Optional[str]


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


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def stop_handler(signum, _frame) -> None:
    global STOP
    LOGGER.info("received signal %s, stopping after current operation", signum)
    STOP = True


def connect_ftp(host: str, user: str, password: str, timeout: int) -> ftplib.FTP:
    ftp = ftplib.FTP()
    ftp.connect(host=host, timeout=timeout)
    ftp.login(user=user, passwd=password)
    ftp.set_pasv(True)
    return ftp


def parse_mlsd_entry(base_path: str, name: str, facts: Dict[str, str]) -> RemoteFile:
    modified = facts.get("modify")
    if modified:
        modified = datetime.strptime(modified, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc).isoformat()

    size = facts.get("size")
    return RemoteFile(
        path=f"{base_path.rstrip('/')}/{name}",
        name=name,
        size=int(size) if size and size.isdigit() else None,
        modified=modified,
    )


def parse_list_line(base_path: str, line: str) -> Optional[RemoteFile]:
    # Typical UNIX LIST line:
    # -rw-r--r-- 1 ftp ftp 123 Jan 01 12:00 LOWW.TXT
    parts = line.split(maxsplit=8)
    if len(parts) < 9 or parts[0].startswith("d"):
        return None

    size = int(parts[4]) if parts[4].isdigit() else None
    name = parts[8]
    return RemoteFile(path=f"{base_path.rstrip('/')}/{name}", name=name, size=size, modified=None)


def matches_any(name: str, patterns: List[str]) -> bool:
    return any(fnmatch.fnmatch(name, pattern) for pattern in patterns)


def list_files(ftp: ftplib.FTP, remote_dir: str, patterns: List[str]) -> List[RemoteFile]:
    files: List[RemoteFile] = []
    try:
        ftp.cwd(remote_dir)
        for name, facts in ftp.mlsd():
            if facts.get("type") == "file" and matches_any(name, patterns):
                files.append(parse_mlsd_entry(remote_dir, name, facts))
    except (ftplib.error_perm, AttributeError):
        lines: List[str] = []
        ftp.cwd(remote_dir)
        ftp.retrlines("LIST", lines.append)
        for line in lines:
            entry = parse_list_line(remote_dir, line)
            if entry and matches_any(entry.name, patterns):
                files.append(entry)

    return sorted(files, key=lambda item: item.name)


def load_state(path: Path) -> Dict[str, Dict[str, object]]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as handle:
            content = handle.read().strip()
    except OSError as error:
        LOGGER.warning("Could not read state file %s, starting fresh: %s", path, error)
        return {}
    if not content:
        return {}
    try:
        return json.loads(content)
    except json.JSONDecodeError as error:
        LOGGER.warning("State file %s is not valid JSON, starting fresh: %s", path, error)
        return {}


def parse_patterns(value: str) -> List[str]:
    patterns = [pattern.strip() for pattern in value.split(",") if pattern.strip()]
    if not patterns:
        raise ValueError("NOAA_FILE_PATTERNS/NOAA_FILE_PATTERN must contain at least one glob")
    return patterns


def save_state(path: Path, state: Dict[str, Dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2, sort_keys=True)
    tmp_path.replace(path)


def file_signature(remote_file: RemoteFile) -> Dict[str, object]:
    return {"size": remote_file.size, "modified": remote_file.modified}


def has_changed(remote_file: RemoteFile, state: Dict[str, Dict[str, object]]) -> bool:
    signature = file_signature(remote_file)
    stored = state.get(remote_file.path)
    if not stored:
        return True
    if not stored.get("completed"):
        return True

    stored_signature = stored.get("signature")
    if stored_signature == signature:
        return False

    if isinstance(stored_signature, dict):
        same_size = stored_signature.get("size") == signature.get("size")
        missing_modified = stored_signature.get("modified") is None
        if same_size and missing_modified:
            return False

    return True


def resume_record_number(remote_file: RemoteFile, state: Dict[str, Dict[str, object]]) -> int:
    stored = state.get(remote_file.path)
    if not stored or stored.get("completed"):
        return 0

    stored_signature = stored.get("signature")
    if isinstance(stored_signature, dict) and stored_signature.get("size") not in {None, remote_file.size}:
        return 0

    records_sent = stored.get("records_sent", 0)
    return int(records_sent) if isinstance(records_sent, int) else 0


def mark_file_progress(
    state_path: Path,
    state: Dict[str, Dict[str, object]],
    remote_file: RemoteFile,
    records_sent: int,
) -> None:
    state[remote_file.path] = {
        "signature": file_signature(remote_file),
        "completed": False,
        "records_sent": records_sent,
        "updated_at": utc_now(),
    }
    save_state(state_path, state)


def retrieve_bytes(ftp: ftplib.FTP, remote_path: str, max_bytes: int) -> bytes:
    chunks: List[bytes] = []
    received = 0

    def collect(chunk: bytes) -> None:
        nonlocal received
        if max_bytes > 0 and received >= max_bytes:
            return
        if max_bytes > 0:
            remaining = max_bytes - received
            chunks.append(chunk[:remaining])
            received += min(len(chunk), remaining)
        else:
            chunks.append(chunk)
            received += len(chunk)

    ftp.retrbinary(f"RETR {remote_path}", collect)
    return b"".join(chunks)


def retrieve_bytes_with_retries(
    host: str,
    user: str,
    password: str,
    timeout: int,
    remote_path: str,
    max_bytes: int,
    attempts: int,
    backoff_seconds: float,
) -> bytes:
    last_error: Optional[BaseException] = None
    for attempt in range(1, max(1, attempts) + 1):
        try:
            with connect_ftp(host, user, password, timeout) as ftp:
                return retrieve_bytes(ftp, remote_path, max_bytes)
        except (ftplib.Error, OSError, TimeoutError) as exc:
            last_error = exc
            if attempt >= max(1, attempts) or STOP:
                break
            sleep_seconds = backoff_seconds * attempt
            LOGGER.warning(
                "download failed for %s, retrying in %.1fs (%s/%s): %s",
                remote_path,
                sleep_seconds,
                attempt,
                max(1, attempts),
                exc,
            )
            time.sleep(sleep_seconds)

    raise RuntimeError(f"failed to download {remote_path} after {max(1, attempts)} attempts") from last_error


def decode_historical_records(remote_file: RemoteFile, payload: bytes) -> List[str]:
    if remote_file.name.endswith(".gz"):
        payload = gzip.decompress(payload)
    return payload.decode("utf-8", errors="replace").splitlines()


def parse_isd_observed_at(record: str) -> Optional[str]:
    if len(record) < 27:
        return None

    date_value = record[15:23]
    time_value = record[23:27]
    try:
        return datetime.strptime(f"{date_value}{time_value}", "%Y%m%d%H%M").replace(tzinfo=timezone.utc).isoformat()
    except ValueError:
        return None


def build_event(source_host: str, remote_file: RemoteFile, record: str, record_number: int, historical_year: str) -> Dict[str, object]:
    return {
        "schema_version": 1,
        "source": {
            "type": "noaa_isd_historical_ftp",
            "host": source_host,
            "path": remote_file.path,
            "file_name": remote_file.name,
            "year": historical_year,
        },
        "observed_at": parse_isd_observed_at(record),
        "ingested_at": utc_now(),
        "size_bytes": remote_file.size,
        "record_number": record_number,
        "payload_format": "isd-fixed-width",
        "payload_text": record,
    }


def create_kafka_producer(bootstrap_servers: str, client_id: str, retries: int) -> KafkaProducer:
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                client_id=client_id,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                key_serializer=lambda value: value.encode("utf-8"),
                acks="all",
                retries=5,
                linger_ms=100,
            )
        except NoBrokersAvailable as exc:
            last_error = exc
            sleep_seconds = min(30, attempt * 2)
            LOGGER.warning("Kafka unavailable, retrying in %ss (%s/%s)", sleep_seconds, attempt, retries)
            time.sleep(sleep_seconds)

    raise RuntimeError(f"could not connect to Kafka at {bootstrap_servers}") from last_error


def changed_files(files: Iterable[RemoteFile], state: Dict[str, Dict[str, object]], max_files: int) -> List[RemoteFile]:
    selected = [remote_file for remote_file in files if has_changed(remote_file, state)]
    if max_files <= 0:
        return selected
    return selected[:max_files]


def poll_once(
    ftp_settings: Tuple[str, str, str, int],
    remote_dir: str,
    patterns: List[str],
    max_files: int,
    max_bytes: int,
    state: Dict[str, Dict[str, object]],
    producer: KafkaProducer,
    topic: str,
    historical_year: str,
    state_path: Path,
    progress_interval_records: int,
    download_retries: int,
    download_retry_backoff_seconds: float,
) -> int:
    host, user, password, timeout = ftp_settings
    sent = 0
    with connect_ftp(host, user, password, timeout) as ftp:
        files = list_files(ftp, remote_dir, patterns)
    pending = changed_files(files, state, max_files)
    file_limit = "all" if max_files <= 0 else str(max_files)
    LOGGER.info("found %s files, %s pending, sending up to %s files", len(files), len(pending), file_limit)

    for index, remote_file in enumerate(pending, start=1):
        if STOP:
            break

        LOGGER.info("downloading file %s/%s: %s", index, len(pending), remote_file.path)
        payload = retrieve_bytes_with_retries(
            host,
            user,
            password,
            timeout,
            remote_file.path,
            max_bytes,
            download_retries,
            download_retry_backoff_seconds,
        )

        records = decode_historical_records(remote_file, payload)
        already_sent = resume_record_number(remote_file, state)
        LOGGER.info("sending %s records from %s (resuming after %s)", len(records), remote_file.path, already_sent)

        futures = []
        last_confirmed_record = already_sent
        for record_number, record in enumerate(records, start=1):
            if record_number <= already_sent:
                continue
            if STOP:
                break
            event = build_event(host, remote_file, record, record_number, historical_year)
            key = f"{remote_file.path}:{record_number}"
            futures.append(producer.send(topic, key=key, value=event))
            sent += 1

            if len(futures) >= progress_interval_records:
                for future in futures:
                    future.get(timeout=30)
                producer.flush(timeout=30)
                futures.clear()
                last_confirmed_record = record_number
                mark_file_progress(state_path, state, remote_file, last_confirmed_record)
                LOGGER.info("progress %s: %s/%s records", remote_file.path, record_number, len(records))

        for future in futures:
            future.get(timeout=30)
        producer.flush(timeout=30)
        if futures:
            last_confirmed_record = record_number

        if last_confirmed_record > already_sent and last_confirmed_record < len(records):
            mark_file_progress(state_path, state, remote_file, last_confirmed_record)
            LOGGER.info("saved partial progress %s: %s/%s records", remote_file.path, last_confirmed_record, len(records))

        if STOP:
            break

        state[remote_file.path] = {
            "signature": file_signature(remote_file),
            "completed": True,
            "records_sent": len(records),
            "completed_at": utc_now(),
        }
        save_state(state_path, state)
        LOGGER.info(
            "completed %s (%s compressed bytes, %s records); progress=%s/%s files",
            remote_file.path,
            remote_file.size,
            len(records),
            index,
            len(pending),
        )

    producer.flush(timeout=30)
    return sent


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    signal.signal(signal.SIGTERM, stop_handler)
    signal.signal(signal.SIGINT, stop_handler)
    socket.setdefaulttimeout(env_int("FTP_TIMEOUT_SECONDS", 30))

    historical_year = os.getenv("NOAA_HISTORICAL_YEAR", "2025")
    ftp_host = os.getenv("NOAA_FTP_HOST", "ftp.ncei.noaa.gov")
    ftp_user = os.getenv("NOAA_FTP_USER", "anonymous")
    ftp_password = os.getenv("NOAA_FTP_PASSWORD", "password")
    ftp_timeout = env_int("FTP_TIMEOUT_SECONDS", 30)
    remote_dir = os.getenv("NOAA_FTP_DIR", f"/pub/data/noaa/{historical_year}")
    file_patterns = parse_patterns(os.getenv("NOAA_FILE_PATTERNS", os.getenv("NOAA_FILE_PATTERN", f"*-{historical_year}.gz")))
    poll_interval = env_float("POLL_INTERVAL_SECONDS", 86400)
    retry_interval = env_float("RETRY_INTERVAL_SECONDS", 60)
    max_files = env_int("MAX_FILES_PER_POLL", 0)
    max_bytes = env_int("MAX_FILE_BYTES", 0)
    progress_interval_records = env_int("PROGRESS_INTERVAL_RECORDS", 1000)
    download_retries = env_int("FTP_DOWNLOAD_RETRIES", 5)
    download_retry_backoff_seconds = env_float("FTP_DOWNLOAD_RETRY_BACKOFF_SECONDS", 5.0)
    run_once = env_bool("RUN_ONCE", True)
    state_path = Path(os.getenv("STATE_FILE", "/app/state/noaa_ftp_state.json"))
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "noaa.weather.raw")
    kafka_client_id = os.getenv("KAFKA_CLIENT_ID", "noaa-ftp-producer")

    LOGGER.info(
        "starting NOAA historical FTP producer: host=%s dir=%s patterns=%s topic=%s year=%s run_once=%s",
        ftp_host,
        remote_dir,
        file_patterns,
        kafka_topic,
        historical_year,
        run_once,
    )
    state = load_state(state_path)
    producer = create_kafka_producer(kafka_bootstrap_servers, kafka_client_id, retries=20)
    ftp_settings = (ftp_host, ftp_user, ftp_password, ftp_timeout)

    while not STOP:
        sleep_interval = poll_interval
        try:
            sent = poll_once(
                ftp_settings=ftp_settings,
                remote_dir=remote_dir,
                patterns=file_patterns,
                max_files=max_files,
                max_bytes=max_bytes,
                state=state,
                producer=producer,
                topic=kafka_topic,
                historical_year=historical_year,
                state_path=state_path,
                progress_interval_records=progress_interval_records,
                download_retries=download_retries,
                download_retry_backoff_seconds=download_retry_backoff_seconds,
            )
            save_state(state_path, state)
            LOGGER.info("poll complete, sent %s records", sent)
            if run_once:
                break
        except Exception as exc:
            LOGGER.exception("poll failed: %s", exc)
            save_state(state_path, state)
            sleep_interval = retry_interval

        slept = 0.0
        while not STOP and slept < sleep_interval:
            sleep_for = min(1.0, sleep_interval - slept)
            time.sleep(sleep_for)
            slept += sleep_for

    save_state(state_path, state)
    producer.close(timeout=30)
    LOGGER.info("producer stopped")


if __name__ == "__main__":
    main()
