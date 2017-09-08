package kafka.examples.wikifeed;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

/**
 * Created by PravinKumar on 29/7/17.
 */
public class WikifeedLambdaexample {
    final static String WIKIFEED_INPUT="wikifeedInput";
    final static String WIKIFEED_OUTPUT="wikifeedOutput";
    final static String WIKIFEED_LAMBDA="WikiFeedLambda";
    final static String BOOTSTRAP_SERVERS="localhost:9092";
    final static String COUNT_STORE="countstore";
    final static String STAT_DIR="/home/admin/Documents/kafka_2.11-0.10.2.1/kafka-streams";

    public static void main(String[] args) {
        KafkaStreams kafkaStreams=getWikifeedStreams();
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    public static KafkaStreams getWikifeedStreams(){

        Properties properties=new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,WIKIFEED_LAMBDA);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,WikifeedSerde.class);
        properties.put(StreamsConfig.STATE_DIR_CONFIG,STAT_DIR);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,500);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder=new KStreamBuilder();
        KStream<String,Wikifeed> inputStream=builder.stream(WIKIFEED_INPUT);
        KTable<String,Long> kTable=inputStream
                .filter((key, value) -> value.isNew())
                .map(((key, value) -> KeyValue.pair(value.getName(),value)))
                .groupByKey()
                .count(COUNT_STORE);
        kTable.to(Serdes.String(), Serdes.Long(),WIKIFEED_OUTPUT);
        KafkaStreams streams= new KafkaStreams(builder,properties);

        return streams;
    }
}
