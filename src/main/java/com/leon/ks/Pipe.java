package com.leon.ks;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class Pipe
{
    public static void main(String[] args) throws Exception
    {
        // create a java.util.Properties map to specify different Streams execution configuration values as defined in StreamsConfig
        Properties props = new Properties();

        // StreamsConfig.APPLICATION_ID_CONFIG gives the unique identifier of your Streams application to distinguish itself with other applications talking to the same Kafka cluster
        // StreamsConfig.BOOTSTRAP_SERVERS_CONFIG specifies a list of host/port pairs to use for establishing the initial connection to the Kafka cluster
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // customize default serialization and deserialization libraries for the record key-value pairs
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream("streams-plaintext-input");
    }
}
