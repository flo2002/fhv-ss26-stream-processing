# fhv-ss26-stream-processing
Processing NOAA weather data with Kafka.

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

