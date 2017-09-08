package kafka.examples.SumLambda;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Collections;
import java.util.Properties;

/**
 * Created by PravinKumar on 18/8/17.
 */
public class SumLambdaDriverExample {


    private static final String CONSUMER_SUM = "consumersum";

    public static void main(String[] args) {
        ProduceInput();
        // ConsumeOutputForEvenNums();
        // ConsumerInputForOddNums();
    }

    public static void ProduceInput() {
        Properties producerProp = new Properties();
        int arr[] = {7, 16, 21, 29, 30, 123, 12, 9, 1, 19, 127, 257, 128};
        producerProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SumLamdaExample.BOOTSTRAP_SERVER);
        KafkaProducer<Integer, Integer> kafkaProducer = new KafkaProducer<Integer, Integer>(producerProp, Serdes.Integer().serializer(), Serdes.Integer().serializer());
        for (int i = 0; i < arr.length; i++) {
            kafkaProducer.send(new ProducerRecord<Integer, Integer>(SumLamdaExample.SUM_INPUT_EVEN_TOPIC, arr[i], arr[i]));
            kafkaProducer.send(new ProducerRecord<Integer, Integer>(SumLamdaExample.SUM_INPUT_ODD_TOPIC, arr[i], arr[i]));

        }
        kafkaProducer.flush();
    }

    private static Properties getConsumerProperties() {
        Properties consumerProp = new Properties();
        consumerProp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SumLamdaExample.BOOTSTRAP_SERVER);
        consumerProp.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_SUM);
        consumerProp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return consumerProp;
    }

    public static void ConsumeOutputForEvenNums() {

        Properties consumerProp = getConsumerProperties();
        KafkaConsumer<Integer, Integer> kafkaConsumer = new KafkaConsumer<Integer, Integer>(consumerProp, Serdes.Integer().deserializer(), Serdes.Integer().deserializer());
        kafkaConsumer.subscribe(Collections.singleton(SumLamdaExample.SUM_OUTPUT_EVEN_TOPIC));
        while (true) {
            kafkaConsumer.poll(Long.MAX_VALUE)
                    .forEach((ConsumerRecord<Integer, Integer> record) -> System.out.println("KEY :" + record.key() + "VALUE:" + record.value()));
        }
    }

    public static void ConsumerInputForOddNums() {
        Properties consumerProp = getConsumerProperties();
        KafkaConsumer<Integer, Integer> kafkaConsumer = new KafkaConsumer<Integer, Integer>(consumerProp, Serdes.Integer().deserializer(), Serdes.Integer().deserializer());
        kafkaConsumer.subscribe(Collections.singleton(SumLamdaExample.SUM_OUTPUT_ODD_TOPIC));
        while (true) {
            kafkaConsumer.poll(Long.MAX_VALUE)
                    .forEach((ConsumerRecord<Integer, Integer> record) -> System.out.println("KEY :" + record.key() + "VALUE:" + record.value()));
        }
    }
}
