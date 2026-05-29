param(
    [string]$ProjectName = "fhv-ss26-stream-processing"
)

$ErrorActionPreference = "Stop"

Write-Host "Containers"
docker ps -a --filter "name=$ProjectName" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

Write-Host ""
Write-Host "Producer checkpoint"
$checkpointScript = @'
import json
from pathlib import Path

path = Path("/state/noaa_ftp_state.json")
if not path.exists():
    print("no checkpoint file yet")
    raise SystemExit

state = json.loads(path.read_text())
completed = [key for key, value in state.items() if value.get("completed")]
partial = [key for key, value in state.items() if not value.get("completed")]
records = sum(int(value.get("records_sent", 0)) for value in state.values())
print(f"tracked_files={len(state)} completed_files={len(completed)} partial_files={len(partial)} records_sent={records}")
if partial:
    for key in partial[:5]:
        print(f"partial {key}: {state[key].get('records_sent', 0)} records")
'@
$checkpointScript | docker run --rm -i -v "${ProjectName}_producer_state:/state" python:3.12-alpine python -

Write-Host ""
Write-Host "Kafka raw topic offsets"
docker exec "${ProjectName}-kafka-1" /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:19092 --topic noaa.weather.raw

Write-Host ""
Write-Host "Dashboard database"
docker exec "${ProjectName}-postgres-1" psql -U noaa -d noaa -c @'
SELECT * FROM noaa_stream_counts ORDER BY kind;
SELECT
  count(DISTINCT station_id) AS station_count,
  count(*) AS station_day_rows,
  min(observation_day) AS first_day,
  max(observation_day) AS last_day
FROM noaa_daily_station_average;
SELECT station_id, count(*) AS days, sum(sample_count) AS samples
FROM noaa_daily_station_average
GROUP BY station_id
ORDER BY station_id
LIMIT 20;
'@
