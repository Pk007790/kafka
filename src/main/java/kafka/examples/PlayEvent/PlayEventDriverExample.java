package kafka.examples.PlayEvent;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by PravinKumar on 31/8/17.
 */
public class PlayEventDriverExample {

    private static final String CONSUMER_ID = "playeventconsumeru15";
    private static final int SENT_RECORDS = 8;

    public static void main(String[] args) {
        sendPlayerEventInput();
        getPlayerEventOutput();
    }

    public static Properties getProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, PlayEventExample.BOOTSTRAP_SERVER);

        return props;
    }

    public static void sendPlayerEventInput() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PlayEventExample.BOOTSTRAP_SERVER);
        KafkaProducer<String, PlayEvent> kafkaProducer = new KafkaProducer<String, PlayEvent>(producerProps, Serdes.String().serializer(), PlayEventSerde.getInstance().serializer());
        Long startTime = System.currentTimeMillis();
        kafkaProducer.send(new ProducerRecord<String, PlayEvent>(PlayEventExample.PLAY_EVENT_TOPIC, null, PlayEventExample.START_TIME - 11, "jio", new PlayEvent(1L, 10L)));
        kafkaProducer.send(new ProducerRecord<String, PlayEvent>(PlayEventExample.PLAY_EVENT_TOPIC, null, PlayEventExample.START_TIME / 10, "redmi", new PlayEvent(2L, 10L)));
        kafkaProducer.send(new ProducerRecord<String, PlayEvent>(PlayEventExample.PLAY_EVENT_TOPIC, null, PlayEventExample.START_TIME / 6, "samsung", new PlayEvent(3L, 10L)));
        kafkaProducer.send(new ProducerRecord<String, PlayEvent>(PlayEventExample.PLAY_EVENT_TOPIC, null, PlayEventExample.START_TIME / 2, "jio", new PlayEvent(1L, 10L)));
        kafkaProducer.send(new ProducerRecord<String, PlayEvent>(PlayEventExample.PLAY_EVENT_TOPIC, null, PlayEventExample.START_TIME - 15, "redmi", new PlayEvent(2L, 10L)));
        kafkaProducer.send(new ProducerRecord<String, PlayEvent>(PlayEventExample.PLAY_EVENT_TOPIC, null, PlayEventExample.START_TIME - 17 , "samsung", new PlayEvent(3L, 10L)));
        kafkaProducer.send(new ProducerRecord<String, PlayEvent>(PlayEventExample.PLAY_EVENT_TOPIC, null, PlayEventExample.START_TIME / 9, "jio", new PlayEvent(1L, 10L)));
        kafkaProducer.send(new ProducerRecord<String, PlayEvent>(PlayEventExample.PLAY_EVENT_TOPIC, null, PlayEventExample.START_TIME - 13, "redmi", new PlayEvent(2L, 10L)));
        kafkaProducer.send(new ProducerRecord<String, PlayEvent>(PlayEventExample.PLAY_EVENT_TOPIC, null, PlayEventExample.START_TIME / 4, "samsung", new PlayEvent(3L, 10L)));
        kafkaProducer.close();
    }
   /* public static void sendPlayerEventInput() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PlayEventExample.BOOTSTRAP_SERVER);
        KafkaProducer<String, PlayEvent> kafkaProducer = new KafkaProducer<String, PlayEvent>(producerProps, Serdes.String().serializer(), PlayEventSerde.getInstance().serializer());
        Long startTime = System.currentTimeMillis();
        kafkaProducer.send(new ProducerRecord<String, PlayEvent>(PlayEventExample.PLAY_EVENT_TOPIC, null, "jio", new PlayEvent(1L, 10L)));
        kafkaProducer.send(new ProducerRecord<String, PlayEvent>(PlayEventExample.PLAY_EVENT_TOPIC, null, "redmi", new PlayEvent(2L, 10L)));
        kafkaProducer.send(new ProducerRecord<String, PlayEvent>(PlayEventExample.PLAY_EVENT_TOPIC, null, "samsung", new PlayEvent(3L, 10L)));
        kafkaProducer.send(new ProducerRecord<String, PlayEvent>(PlayEventExample.PLAY_EVENT_TOPIC, null, "jio", new PlayEvent(1L, 10L)));
        kafkaProducer.send(new ProducerRecord<String, PlayEvent>(PlayEventExample.PLAY_EVENT_TOPIC, null, "redmi", new PlayEvent(2L, 10L)));
        kafkaProducer.send(new ProducerRecord<String, PlayEvent>(PlayEventExample.PLAY_EVENT_TOPIC, null, "samsung", new PlayEvent(3L, 10L)));
        kafkaProducer.send(new ProducerRecord<String, PlayEvent>(PlayEventExample.PLAY_EVENT_TOPIC, null, "jio", new PlayEvent(1L, 10L)));
        kafkaProducer.send(new ProducerRecord<String, PlayEvent>(PlayEventExample.PLAY_EVENT_TOPIC, null, "redmi", new PlayEvent(2L, 10L)));
        kafkaProducer.send(new ProducerRecord<String, PlayEvent>(PlayEventExample.PLAY_EVENT_TOPIC, null, "samsung", new PlayEvent(3L, 10L)));
        kafkaProducer.close();
    }*/

    public static void getPlayerEventOutput() {
        Properties consumerProps = getProperties();
        KafkaConsumer<String,Long> kafkaConsumer = new KafkaConsumer<String, Long>(consumerProps, Serdes.String().deserializer(),Serdes.Long().deserializer());
        kafkaConsumer.subscribe(Collections.singleton(PlayEventExample.PLAY_EVENT_OUTPUT));
        int initialRecord=0;
        while(initialRecord<SENT_RECORDS){
            ConsumerRecords<String, Long> records=kafkaConsumer.poll(Long.MAX_VALUE);
            records.forEach(record -> {
                String[] windows = record.key().split("@")[1].split("->");
                long timeDiff = Long.parseLong(windows[1]) - Long.parseLong(windows[0]);
                System.out.println(record.key()+" : "+record.value() + ", diff : " + TimeUnit.MILLISECONDS.toSeconds(timeDiff)+" seconds"+", Start time ="+new Date(Long.parseLong(windows[0]))+", End Time ="+new Date(Long.parseLong(windows[1])));
            });
            initialRecord+=records.count();
        }
        kafkaConsumer.close();
    }
}
