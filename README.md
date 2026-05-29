# fhv-ss26-stream-processing
Repo Description: Processing NOAA weather data with Kafka.

## Ideas to poll the data via FTP
- Kafka Connect with FTP Source Connector
  - is probably not good because of licensing issues
- Custom Python script that reads the historical data and sends it to Kafka
  - is probably the most flexible, then we can track which archive files were already processed and resume without sending duplicates for completed files

### Doing some Research
Questions:
- What's the difference between Kafka Topics and Streams?
  - Kafka Topics are the basic unit of data storage in Kafka, they are like a log where producers write data and consumers read data. Streams are a higher-level abstraction that allows you to process data in real-time as it flows through Kafka, they provide a way to perform operations on the data such as filtering, mapping, and aggregating.

### Setting up Kafka
- We use the Kafka Docker image to set it up locally on each machine 
  - We have two Macs and two Windows machines in our team, so containerization is probably a good idea.
- Which NOAA data do we use?
  - There is no live 2026 dataset for our use case, so we read the historical data from 2025.
  - The producer reads the NCEI archive from `ftp.ncei.noaa.gov` at `/pub/data/noaa/2025`.
  - The historical 2025 archive is about 1.5 GB in total.


## Run the Starter Container
```powershell
docker compose up --build
```

The producer reads historical NOAA ISD station-year files for 2025 and writes JSON messages to Kafka topic `noaa.weather.raw`.

To read messages:
```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic noaa.weather.raw --from-beginning
```

## Java Kafka Streams Client
The Maven project in `patterns` consumes the NOAA Kafka topic, deserializes the JSON envelope, parses the text payload.

Defaults:
- Kafka bootstrap server: `localhost:19094`
- Input topic: `noaa.weather.raw`
- Output topic for daily averages: `noaa.weather.daily-average-temperature`

Run it from `patterns`:
```powershell
mvn exec:java
```

For a fresh `KAFKA_STREAMS_APPLICATION_ID`, the stream starts at the beginning of the topic so it can process the already-produced 2025 historical data. So, every time the Java application is started, the full data from Kafka is reprocessed.

## Task Distribution
round robin with:
- 1 Florian, 2 Chris, 3 Mykola, 4 Haroldas

# Patterns
## Pattern 1: compute the average temperatures for each stations per day in 2025 (Florian)
thoughts:
- Python is no option (further info from Haroldas) --> Java (especially with Java/Kafka Streams) is used.
- Where should parsed data go? --> Into a new topic `noaa.weather.parsed` with a more structured format (e.g. JSON with fields for station, date, temperature, etc.)
- How to visualize the results?


