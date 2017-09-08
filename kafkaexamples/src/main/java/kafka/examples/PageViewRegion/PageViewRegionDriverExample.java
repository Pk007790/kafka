package kafka.examples.PageViewRegion;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.io.Serializable;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.stream.IntStream;

/**
 * Created by PravinKumar on 7/9/17.
 */
public class PageViewRegionDriverExample {


    public static final String CONSUMER_ID = "pageviewconsumer";

    public static void main(String[] args) {
        //producerInput();
        consumerOutput();
    }

    public static Properties getConsumerProps() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, PageViewRegionLambdaExample.BOOTSTRAP_SERVER);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.Long().deserializer());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_ID);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return properties;
    }

    public static void producerInput() {

        final String[] users = {"erica", "bob", "joe", "damian", "tania", "phil", "sam", "lauren", "joseph"};
        final String[] regions = {"europe", "usa", "asia", "africa"};
        Random random = new Random();
        Properties producerProps = new Properties();
        producerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, PageViewRegionLambdaExample.BOOTSTRAP_SERVER);
        KafkaProducer<String, Serializable> producer = new KafkaProducer<String, Serializable>(producerProps, Serdes.String().serializer(), MySerde.getSerde().serializer());
        for (String user : users) {
            producer.send(new ProducerRecord<String, Serializable>(PageViewRegionLambdaExample.USER_PROFILE, user, new UserProfile("some", regions[random.nextInt(regions.length)])));
            IntStream.range(0, random.nextInt(10))
                    .forEach(record -> producer.send(new ProducerRecord<String, Serializable>(PageViewRegionLambdaExample.PAGE_VIEW, null, new PageView(user, "index.html"))));
        producer.flush();

        }
    }

    public static void consumerOutput() {
        Properties consumerProps = getConsumerProps();
        KafkaConsumer<String, Long> consumer = new KafkaConsumer<String, Long>(consumerProps, Serdes.String().deserializer(), Serdes.Long().deserializer());
        consumer.subscribe(Collections.singleton(PageViewRegionLambdaExample.VIEW_REGION));
        while (true) {
            consumer.poll(Long.MAX_VALUE).forEach(ConsumerRecord -> System.out.println("Key : " + ConsumerRecord.key() + "Value : " + ConsumerRecord.value()));
        }

    }
}
