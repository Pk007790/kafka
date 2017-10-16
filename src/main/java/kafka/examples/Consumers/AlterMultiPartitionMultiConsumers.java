package kafka.examples.Consumers;

import kafka.examples.PageViewRegion.PageViewRegionLambdaExample;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Collections;
import java.util.Properties;

/* Topic:inputtopicmul Partition:5  Consumer:3
*   Partitions altered from 5 to 10
* */

/**
 * Created by PravinKumar on 13/10/17.
 */

public class AlterMultiPartitionMultiConsumers {

    public static final String CONSUMER_GROUP_ID = "altermultipartitionmulticonsumer2";
    public static String INPUT_TOPIC = "inputtopicalt2";
    public static void main(String[] args) throws InterruptedException {
        consumerOutput();
    }

    public static Properties getConsumerProps() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, PageViewRegionLambdaExample.BOOTSTRAP_SERVER);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass().getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass().getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,100);
        properties.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG,10000);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG,"C1");
        //properties.put(ConsumerConfig.CLIENT_ID_CONFIG,"C2");
        //properties.put(ConsumerConfig.CLIENT_ID_CONFIG,"C3");
        //properties.put(ConsumerConfig.CLIENT_ID_CONFIG,"C4");

        return properties;
    }


    public static void consumerOutput() throws InterruptedException {
        Properties consumerProps = getConsumerProps();
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(consumerProps, Serdes.String().deserializer(), Serdes.String().deserializer());
        consumer.subscribe(Collections.singleton(INPUT_TOPIC));
        while (true) {
            Thread.sleep(5_000);
            consumer.poll(Long.MAX_VALUE).forEach(ConsumerRecord -> System.out.println("Partition :"+ ConsumerRecord.partition() +"------------"+"Key : " + ConsumerRecord.key() +"------------"+ "Value : " + ConsumerRecord.value()));
        }

    }
}
