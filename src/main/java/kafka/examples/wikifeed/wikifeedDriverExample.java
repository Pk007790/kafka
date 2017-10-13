package kafka.examples.wikifeed;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.stream.IntStream;

/**
 * Created by PravinKumar on 29/7/17.
 */
public class wikifeedDriverExample {

    final static String BOOTSTRAP_SERVERS="localhost:9092";
    final static String CONSUMER_WIKIFEED_LAMBDA="ConsumerWikiFeedLambda";

    public static void main(String[] args) {
        ProducerInput();
        ConsumerOutput();
    }

    public static void ProducerInput(){
        String[] users={"pravin","kumar","erica", "bob", "joe", "damian", "tania", "phil", "sam",
                "lauren", "joseph"};

        Properties properties=new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,WikifeedSerde.getInstance().serializer().getClass());

        KafkaProducer<String,Wikifeed> producer=new KafkaProducer<String, Wikifeed>(properties);
        Random random=new Random();
        IntStream.range(0,random.nextInt(100))
                .mapToObj(value -> new Wikifeed(users[random.nextInt(users.length)],true,"content"))
                .forEach(record -> producer.send(new ProducerRecord<String, Wikifeed>(WikifeedLambdaexample.WIKIFEED_INPUT,null,record)));
        producer.flush();
    }

    public static void ConsumerOutput() {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,CONSUMER_WIKIFEED_LAMBDA);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, Long> consumer = new KafkaConsumer<String, Long>(properties, new StringDeserializer(), new LongDeserializer());
        consumer.subscribe(Collections.singleton(WikifeedLambdaexample.WIKIFEED_OUTPUT));

        while (true) {
            consumer.poll(Long.MAX_VALUE)
                    .forEach((ConsumerRecord<String, Long> consumerRecord) -> System.out.println("Key:"+consumerRecord.key()+"="+"Value:"+consumerRecord.value()));
        }
    }
}
