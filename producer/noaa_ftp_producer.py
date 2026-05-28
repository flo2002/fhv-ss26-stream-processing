import fnmatch
import ftplib
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
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


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
    return state.get(remote_file.path) != file_signature(remote_file)


def retrieve_text(ftp: ftplib.FTP, remote_path: str, max_bytes: int) -> str:
    chunks: List[bytes] = []
    received = 0

    def collect(chunk: bytes) -> None:
        nonlocal received
        if received >= max_bytes:
            return
        remaining = max_bytes - received
        chunks.append(chunk[:remaining])
        received += min(len(chunk), remaining)

    ftp.retrbinary(f"RETR {remote_path}", collect)
    return b"".join(chunks).decode("utf-8", errors="replace")


def build_event(source_host: str, remote_file: RemoteFile, content: str) -> Dict[str, object]:
    return {
        "schema_version": 1,
        "source": {
            "type": "noaa_ftp",
            "host": source_host,
            "path": remote_file.path,
            "file_name": remote_file.name,
        },
        "observed_at": remote_file.modified,
        "ingested_at": utc_now(),
        "size_bytes": remote_file.size,
        "payload_text": content,
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
) -> int:
    host, user, password, timeout = ftp_settings
    sent = 0
    with connect_ftp(host, user, password, timeout) as ftp:
        files = list_files(ftp, remote_dir, patterns)
        pending = changed_files(files, state, max_files)
        LOGGER.info("found %s files, %s changed, sending up to %s", len(files), len(pending), max_files)

        for remote_file in pending:
            if STOP:
                break
            content = retrieve_text(ftp, remote_file.path, max_bytes)
            event = build_event(host, remote_file, content)
            future = producer.send(topic, key=remote_file.path, value=event)
            future.get(timeout=30)
            state[remote_file.path] = file_signature(remote_file)
            sent += 1
            LOGGER.info("sent %s (%s bytes)", remote_file.path, remote_file.size)

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

    ftp_host = os.getenv("NOAA_FTP_HOST", "tgftp.nws.noaa.gov")
    ftp_user = os.getenv("NOAA_FTP_USER", "anonymous")
    ftp_password = os.getenv("NOAA_FTP_PASSWORD", "anonymous@example.com")
    ftp_timeout = env_int("FTP_TIMEOUT_SECONDS", 30)
    remote_dir = os.getenv("NOAA_FTP_DIR", "/data/observations/metar/stations")
    file_patterns = parse_patterns(os.getenv("NOAA_FILE_PATTERNS", os.getenv("NOAA_FILE_PATTERN", "E*.TXT,L*.TXT")))
    poll_interval = env_float("POLL_INTERVAL_SECONDS", 900)
    max_files = env_int("MAX_FILES_PER_POLL", 50)
    max_bytes = env_int("MAX_FILE_BYTES", 262144)
    state_path = Path(os.getenv("STATE_FILE", "/app/state/noaa_ftp_state.json"))
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "noaa.weather.raw")
    kafka_client_id = os.getenv("KAFKA_CLIENT_ID", "noaa-ftp-producer")

    LOGGER.info("starting NOAA FTP producer: host=%s dir=%s patterns=%s topic=%s", ftp_host, remote_dir, file_patterns, kafka_topic)
    state = load_state(state_path)
    producer = create_kafka_producer(kafka_bootstrap_servers, kafka_client_id, retries=20)
    ftp_settings = (ftp_host, ftp_user, ftp_password, ftp_timeout)

    while not STOP:
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
            )
            save_state(state_path, state)
            LOGGER.info("poll complete, sent %s records", sent)
        except (ftplib.all_errors, KafkaError, OSError, RuntimeError) as exc:
            LOGGER.exception("poll failed: %s", exc)

        slept = 0.0
        while not STOP and slept < poll_interval:
            sleep_for = min(1.0, poll_interval - slept)
            time.sleep(sleep_for)
            slept += sleep_for

    save_state(state_path, state)
    producer.close(timeout=30)
    LOGGER.info("producer stopped")


if __name__ == "__main__":
    main()
