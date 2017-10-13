package kafka.examples.streams.wordcount;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;

/**
 * Created by pravinkumar on 21/7/17.
 */
public class MapFunction {
    public static void main(String[] args) {
        final Properties streamConfiguration = new Properties();
        streamConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "MapFunctionExample");
        streamConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamConfiguration.put(StreamsConfig.STATE_DIR_CONFIG,"/home/admin/Documents/kafka_2.11-0.10.2.1/kafka-streams");
        streamConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);
        final KStreamBuilder builder = new KStreamBuilder();
        final KStream<String, String> topic1 = builder.stream("t1");
        final KStream<String,String> topic2=topic1.map((nullkey, value) -> KeyValue.pair(value, value.toUpperCase()));
        topic2.groupByKey().count();
        topic2.to("t2");
        final KafkaStreams streams=new KafkaStreams(builder,streamConfiguration);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
