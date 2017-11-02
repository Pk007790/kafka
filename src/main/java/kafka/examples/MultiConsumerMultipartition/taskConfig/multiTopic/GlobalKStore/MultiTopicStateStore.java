package kafka.examples.MultiConsumerMultipartition.taskConfig.multiTopic.GlobalKStore;

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
public class MultiTopicStateStore {

    public static final String APPLICATION_ID="wc-exampley";
    public static final String BOOTSTRAP_SERVER="localhost:9092";
    public static final String INPUT_TOPICA="a";
    public static final String INPUT_TOPICB="b";
    public static final String INPUT_TOPICC="c";
    public static final String OUTPUT_TOPIC="opy";

    private static final String INPUT_TABLE="gkt-tabley";
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
        properties.put(StreamsConfig.STATE_DIR_CONFIG,"/home/admin/Documents/kafka_2.11-0.10.2.1/kafka-streams-ab");
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        return properties;
    }
    public static KafkaStreams getStreams(){
        Properties props=getProps();
        KStreamBuilder builder=new KStreamBuilder();
        // each input topic (a, b, c) has 3 partitions. I'm running the KafkaStreams DSL word count example
        // in multiple instances.
        KStream<String, Long> inputStream = builder.<String,String>stream(INPUT_TOPICA,INPUT_TOPICB,INPUT_TOPICC)
                .map(((key, value) -> new KeyValue<>(value, value)))
                .groupByKey()
                .count(INPUT_TABLE) // create KTable. Is there any option to create Global KTable here ?
                .toStream();
        inputStream.to(Serdes.String(),Serdes.Long(),OUTPUT_TOPIC);

        System.out.println("inputStream {}"+inputStream);

        // output topic with 1 partition
        /*KStream<String, Long> stringLongKStream = builder.stream(Serdes.String(), Serdes.Long(), OUTPUT_TOPIC).groupByKey().reduce((value1, value2) -> value1 + value2).toStream();
        System.out.println("stringLongKStream {}" +stringLongKStream);
        stringLongKStream.foreach((key, value) -> System.out.println(key + "->" +value));*/

        //GlobalKTable<Object, Object> objectObjectGlobalKTable = builder.globalTable(OUTPUT_TOPIC);
        KafkaStreams kafkaStreams=new KafkaStreams(builder,props);
        return kafkaStreams;
    }
}
