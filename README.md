# fhv-ss26-stream-processing
Repo Description: Processing NOAA weather data with Kafka.

## Ideas to poll the data via FTP
- Kafka Connect with FTP Source Connector
  - is probably not good because of licensing issues
- Custom Python script that polls the data and sends it to Kafka 
  - is probably the most flexible, then we could also poll based on the timestamp and get only the new data, but we would have to implement the logic ourselves

### Doing some Research
Questions:
- What's the difference between Kafka Topics and Streams?
  - Kafka Topics are the basic unit of data storage in Kafka, they are like a log where producers write data and consumers read data. Streams are a higher-level abstraction that allows you to process data in real-time as it flows through Kafka, they provide a way to perform operations on the data such as filtering, mapping, and aggregating.

### Setting up Kafka
- We use the Kafka Docker image to set it up locally on each machine 
  - We have two Macs and two Windows machines in our team, so containerization is probably a good idea.
- How often should we poll the data? How much data is that? Aren't we hitting the limit of the FTP server if we poll too often?
  - Polls every 15 minutes.
  - Sends at most 50 changed files per poll, reading max 256 KiB per file.
  - Data is updated every hour: Source: https://www.weather.gov/tg/datahelp
- Is there a close FTP server that would be faster to poll from?
  - Yes, there is a European one: `tgftp.nws.noaa.gov`
- How about old data? Do we want to poll it as well? How much data is that?


## Run the Starter Container
```powershell
docker compose up --build
```

The producer polls European METAR station files from `tgftp.nws.noaa.gov` and writes JSON messages to Kafka topic `noaa.weather.raw`.

To read messages:
```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic noaa.weather.raw --from-beginning
```
## Task Distribution
round robin with:
- 1 Florian, 2 Chris, 3 Mykola, 4 Haroldas
