package kafka.examples.streams.wordcount;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;


/**
 * Created by pravinkumar on 19/7/17.
 */
public class AnomalyDetectionLambdaExample {
    public static void main(final String[] args) throws Exception {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "anomaly-detection-lambda-example");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG,"/home/admin/Documents/kafka_2.11-0.10.2.1/kafka-streams");
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);
        Serde<String> stringSerde=Serdes.String();
        Serde<Long> longSerde=Serdes.Long();

        final KStreamBuilder builder = new KStreamBuilder();
        final KStream<String, String> topic1 = builder.stream("pravin");
        final KTable<Windowed<String>,Long> kTable=topic1
                .map((key, value) -> KeyValue.pair(value,value))
                .groupByKey()
                .count((TimeWindows.of(60*1000L)),"count")
                .filter((countid,count) ->count>=3);
        final KStream<String,Long> topic2=kTable.toStream()
                .filter((windowid,count) -> count!=null)
                .map((windowid,count) ->new KeyValue<>(windowid.toString(),count));
        topic2.to(stringSerde, longSerde,"kumar");

        final KafkaStreams streams=new KafkaStreams(builder,streamsConfiguration);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}