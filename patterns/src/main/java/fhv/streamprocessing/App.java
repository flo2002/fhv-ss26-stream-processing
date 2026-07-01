package fhv.streamprocessing;

import fhv.streamprocessing.kafka.NoaaWeatherStreamApp;

/**
 * Command-line entry point that starts the configured NOAA stream-processing application.
 */
public class App {
    public static void main(String[] args) {
        NoaaWeatherStreamApp.main(args);
    }
}
