package kafka.examples.MultiConsumerMultipartition.taskConfig.multiTopic;

import kafka.examples.MultiConsumerMultipartition.taskConfig.MultithreadInSingleJVM;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.stream.IntStream;

/**
 * Created by PravinKumar on 31/10/17.
 */
public class MultiTopicDriverExample {
    public static final String CONSUMER_GROUP_ID = "multitopiccons";
    private static final int MAX_RECORDS=10000;
    public static void main(String[] args) throws InterruptedException {
        produceInput();
        consumerOutput();
    }

    public static Properties getConsumerProps() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, MutliTopicExample.BOOTSTRAP_SERVER);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass().getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.Long().deserializer().getClass().getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,10);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG,"C1");
        //properties.put(ConsumerConfig.CLIENT_ID_CONFIG,"C2");
        //properties.put(ConsumerConfig.CLIENT_ID_CONFIG,"C3");

        return properties;
    }

    public static void produceInput(){
        Random random=new Random();
        String[] msg={"hi","my","name","is","pravin","kumar","studied","in","madras","institute","of","technology"
                ,"hi","my","name","is","pravin","kumar","studied","in","good","shepherd","school","properties","put"
                ,"ConsumerConfig","BOOTSTRAP","SERVERS","CONFIG","Single","Partition","MultiConsumer","UsingStream"
                , "BOOTSTRAP","SERVER","properties","put","StreamsConfig","DEFAULT","KEY","SERDE","CLASS","CONFIG"
                ,"Serdes","String","getClass","getName"};
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, MultithreadInSingleJVM.BOOTSTRAP_SERVER);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,Serdes.String().serializer().getClass().getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,Serdes.String().serializer().getClass().getName());
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(producerProps);
        IntStream.range(0,MAX_RECORDS)
                .forEach(record ->producer.send(new ProducerRecord<String, String>
                        (MutliTopicExample.INPUT_TOPICA,null,msg[random.nextInt(msg.length)])));
        IntStream.range(0,MAX_RECORDS)
                .forEach(record ->producer.send(new ProducerRecord<String, String>
                        (MutliTopicExample.INPUT_TOPICB,null,"pravin")));
        IntStream.range(0,MAX_RECORDS)
                .forEach(record ->producer.send(new ProducerRecord<String, String>
                        (MutliTopicExample.INPUT_TOPICC,null,"kumar")));

        producer.flush();
    }
    public static void consumerOutput() throws InterruptedException {
        Properties consumerProps = getConsumerProps();
        KafkaConsumer<String,Long> consumer = new KafkaConsumer<String, Long>(consumerProps);
        consumer.subscribe(Collections.singleton(MultithreadInSingleJVM.OUTPUT_TOPIC));
        while (true) {
            Thread.sleep(5_000);
            consumer.poll(Long.MAX_VALUE).forEach(ConsumerRecord ->
                    System.out.println("Partition :"+ConsumerRecord.partition()+"Key : " + ConsumerRecord.key() + "Value : " + ConsumerRecord.value()));
        }

    }
}
