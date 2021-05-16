package com.leon.ks;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class LineSplit
{
    public static void main(String[] args) throws Exception
    {
        // create a java.util.Properties map to specify different Streams execution configuration values as defined in StreamsConfig
        Properties props = new Properties();

        // StreamsConfig.APPLICATION_ID_CONFIG gives the unique identifier of your Streams application to distinguish itself with other applications talking to the same Kafka cluster
        // StreamsConfig.BOOTSTRAP_SERVERS_CONFIG specifies a list of host/port pairs to use for establishing the initial connection to the Kafka cluster
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-linesplit");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // customize default serialization and deserialization libraries for the record key-value pairs
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Processing logic is defined as a topology of connected processor nodes. We can use a topology builder to construct such a topology
        final StreamsBuilder builder = new StreamsBuilder();

        // create a source stream from a Kafka topic named streams-plaintext-input using this topology builder
        KStream<String, String> source = builder.stream("streams-plaintext-input");
        // Since each of the source stream's record is a String typed key-value pair, let's treat the value
        // string as a text line and split it into words with a FlatMapValues operator.
        // Generates a new stream by processing each record from its source stream in order and breaking its value
        // string into a list of words, and producing each word as a new record to the output stream.
        // This is a stateless operator that does not need to keep track of any previously received records or processed results
        source.flatMapValues(value -> Arrays.asList(value.split("\\W+"))).to("streams-linesplit-output");

        // We can inspect what kind of topology is created from this builder by doing the following:
        final Topology topology = builder.build();
        // And print its description to standard output as:
        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);
        // The countdown latch is used to make the main thread wait for the exit until Ctrl-C has been input via the shutdown hook.
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook")
        {
            @Override
            public void run()
            {
                streams.close();
                latch.countDown();
            }
        });

        try
        {
            streams.start();
            latch.await();
        }
        catch (Throwable e)
        {
            System.exit(1);
        }
        System.exit(0);
    }
}
