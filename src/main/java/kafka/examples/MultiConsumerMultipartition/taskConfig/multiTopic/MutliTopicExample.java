package kafka.examples.MultiConsumerMultipartition.taskConfig.multiTopic;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;

/**
 * Created by PravinKumar on 31/10/17.
 */
public class MutliTopicExample {

    public static final String APPLICATION_ID="MultiTopic";
    public static final String BOOTSTRAP_SERVER="localhost:9092";
    public static final String INPUT_TOPICA="multitopicin1";
    public static final String INPUT_TOPICB="multitopicin2";
    public static final String INPUT_TOPICC="multitopicin3";
    public static final String OUTPUT_TOPIC="multitopicout";
    private static final String INPUT_TABLE="multitopictable";
    public static void main(String[] args) {
        KafkaStreams streams=getStreams();
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static Properties getProps(){
        Properties properties=new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,APPLICATION_ID);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass().getName());
        properties.put(StreamsConfig.STATE_DIR_CONFIG,"/home/admin/Documents/kafka_2.11-0.10.2.1/kafka-streams");
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        return properties;
    }
    public static KafkaStreams getStreams(){
        Properties props=getProps();
        KStreamBuilder builder=new KStreamBuilder();
        KStream<String, Long> inputStream = builder.<String,String>stream(INPUT_TOPICA,INPUT_TOPICB,INPUT_TOPICC)
                .map(((key, value) -> new KeyValue<>(value, value)))
                .groupByKey()
                .count(INPUT_TABLE)
                .toStream();
        inputStream.to(Serdes.String(),Serdes.Long(),OUTPUT_TOPIC);
        KafkaStreams kafkaStreams=new KafkaStreams(builder,props);
        return kafkaStreams;
    }
}
