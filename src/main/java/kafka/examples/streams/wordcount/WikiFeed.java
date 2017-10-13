package kafka.examples.streams.wordcount;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.stream.IntStream;

/**
 * Created by PravinKumar on 25/7/17.
 */
public class WikiFeed {
    final static String TOPIC_1="topic1";
    final  static String TOPIC_2="topic2";
    public static void producerInput(Properties prodProperties){
        String [] users={"seb","bal","nis","pravin","erica","bob","joe","damian","tania","phil", "sam","lauren", "joseph"};
        KafkaProducer<String,byte[]> kafkaProducer=new KafkaProducer<>(prodProperties);
        Random random=new Random();
        IntStream.range(0,random.nextInt(100))
                .mapToObj((value -> users[random.nextInt(users.length)]+random.nextInt(users.length)))
                .forEach(record -> kafkaProducer.send(new ProducerRecord<String, byte[]>(TOPIC_1,null,record.getBytes())));
    }

    public static void process(KStreamBuilder builder,Properties properties){

        KStream<String,byte[]> stream = builder.stream(TOPIC_1);

        KTable<String,Long> ktable =stream
                .map((key, value) -> KeyValue.pair(value.toString(),value))
                .groupByKey()
                .count("Count");
        ktable.to(TOPIC_2);
        KafkaStreams streams=new KafkaStreams(builder,properties);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
    public static void consumerOutput(Properties consProperties,KStreamBuilder builder){
        KafkaConsumer<String,Long> consumer=new KafkaConsumer<String, Long>(consProperties,new StringDeserializer(),new LongDeserializer());
        consumer.subscribe(Collections.singleton("topic2"));
        ConsumerRecords<String, Long> record=consumer.poll(Long.MAX_VALUE);
        for (ConsumerRecord<String,Long> consumerRecord:record) {
            System.out.println("key"+consumerRecord.key()+"  "+"Value"+consumerRecord.value());
        }
    }
    public static void main(String[] args) {

        Properties properties=new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"WIKIFEED");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.ByteArray().getClass().getName());
        properties.put(StreamsConfig.STATE_DIR_CONFIG,"/home/admin/Documents/kafka_2.11-0.10.2.1/kafka-streams");
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,500);
        KStreamBuilder builder=new KStreamBuilder();
        producerInput(properties);
        process(builder,properties);
        consumerOutput(properties,builder);
    }

}
