package kafka.examples.UserRegion;

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
 * Created by PravinKumar on 29/8/17.
 */
public class UserRegionLambdaExample {

    private static final String USER_REGION_ID = "userregion";
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String STAT_DIR = "/home/admin/Documents/kafka_2.11-0.10.2.1/kafka-streams";
    private static final String PAGE_VIEW_TOPIC = "pageviewx";
    private static final String PAGE_VIEW_QS = "pageviewquerystore";
    private static final String PAGE_VIEW_STORE = "pageviewstore";
    private static final String PAGE_OUTPUT_STREAM = "pageoutputstream";


    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, USER_REGION_ID);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.STATE_DIR_CONFIG, STAT_DIR);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();
        KTable<String, String> kTable = builder.table(PAGE_VIEW_TOPIC, PAGE_VIEW_QS);
        KTable<String, Long> pageStore = kTable.groupBy(((key, value) -> KeyValue.pair(value, value)))
                .count(PAGE_VIEW_STORE)
                .filter((regionName, count) -> count >= 2);
        KStream<String, Long> pageOutputStream = pageStore.toStream()
                .filter((regionName, count) -> count != null);
        pageOutputStream.to(Serdes.String(), Serdes.Long(), PAGE_OUTPUT_STREAM);

        KafkaStreams streams = new KafkaStreams(builder, properties);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
