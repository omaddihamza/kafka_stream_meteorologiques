package com.me;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;


import java.util.Locale;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("application.id", "Donnees-Meteorologiques");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("default.key.serde", Serdes.String().getClass().getName());
        props.put("default.value.serde", Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> weatherStream = builder.stream("weather-data");

        KStream<String, String> filteredTransformed = weatherStream
                .mapValues(value -> value.trim())
                .filter((key, value) -> value.split(",").length == 3)
                .map((key, value) -> {
                    String[] parts = value.split(",");
                    return KeyValue.pair(parts[0], value); // key = station
                })
                .filter((station, value) -> {
                    String[] parts = value.split(",");
                    double temperature = Double.parseDouble(parts[1]);
                    return temperature > 30.0;
                })
                .mapValues(value -> {
                    String[] parts = value.split(",");
                    String station = parts[0];
                    double celsius = Double.parseDouble(parts[1]);
                    double humidity = Double.parseDouble(parts[2]);

                    double fahrenheit = (celsius * 9 / 5) + 32;
                    return String.format(Locale.US, "%s,%.2f,%.2f", station, fahrenheit, humidity);
                });

        // Group by station and aggregate
        KTable<String, String> aggregated = filteredTransformed
                .groupByKey()
                .aggregate(
                        () -> "0.0,0.0,0", // Initializer: sumTemp,sumHumidity,count
                        (station, newValue, aggregate) -> {
                            String[] newParts = newValue.split(",");
                            double temp = Double.parseDouble(newParts[1]);
                            double humidity = Double.parseDouble(newParts[2]);

                            String[] aggParts = aggregate.split(",");
                            double sumTemp = Double.parseDouble(aggParts[0]);
                            double sumHumidity = Double.parseDouble(aggParts[1]);
                            int count = Integer.parseInt(aggParts[2]);

                            sumTemp += temp;
                            sumHumidity += humidity;
                            count += 1;

                            return String.format(Locale.US, "%.2f,%.2f,%d", sumTemp, sumHumidity, count);
                        },
                        Materialized.with(Serdes.String(), Serdes.String())
                )
                .mapValues((key, agg) -> {
                    String[] parts = agg.split(",");
                    double sumTemp = Double.parseDouble(parts[0]);
                    double sumHum = Double.parseDouble(parts[1]);
                    int count = Integer.parseInt(parts[2]);

                    double avgTemp = sumTemp / count;
                    double avgHum = sumHum / count;

                    return String.format("%s : Température Moyenne = %.2f°F, Humidité Moyenne = %.2f%%", key, avgTemp, avgHum);
                });


        // Publish results to output topic
        aggregated.toStream().to("station-averages", Produced.with(Serdes.String(), Serdes.String()));

        // Build and start the stream
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Graceful shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.start();
    }
}
