package kafka.examples.GlobalKTable;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Collections;
import java.util.Properties;
import java.util.Random;



public class GlobalKTableDriverExample {

    private static final String BOOTSTRAP_SERVER="localhost:9092";
    private static final String ORDER_TOPIC = "ordertopic";
    private static final String CUSTOMER_TOPIC="customertopic";
    private static final String PRODUCT_TOPIC="producttopic";
    private static final String ENRICHED_ORDER_TOPIC="enrichedordertopic";
    private static final String GKT_CONSUMER_ID="consumerId";
    private static final int MAX_NUM=100;
    private static final Random random=new Random();

    public static void main(String[] args) {
        generateOrderInput();
        generateCustomerInput();
        generateProductInput();
        receiveEnrichedOutput();
    }


    public static void generateOrderInput(){
        Properties properties=new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        KafkaProducer<Long,Order> orderKafkaProducer=new KafkaProducer<Long, Order>(properties,Serdes.Long().serializer(),GlobalKtableUtil.getOrderSerde().serializer());
        for (long i = 0; i <MAX_NUM ; i++) {
            Order order=new Order(random.nextInt(MAX_NUM),random.nextInt(MAX_NUM),random.nextLong());
            orderKafkaProducer.send(new ProducerRecord<Long,Order>(ORDER_TOPIC,i,order));
        }
        orderKafkaProducer.flush();
    }

    public static void generateCustomerInput(){
        Properties properties=new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        String [] genders={"Male","Female","Unknown"};
        KafkaProducer<Long,Customer> customerKafkaProducer=new KafkaProducer<Long, Customer>(properties,Serdes.Long().serializer(),GlobalKtableUtil.getCustomerSerde().serializer());
        for (long i = 0; i <MAX_NUM ; i++) {
            Customer customer=new Customer(randomString(10),genders[random.nextInt(genders.length)],randomString(20));
            customerKafkaProducer.send(new ProducerRecord<Long,Customer>(CUSTOMER_TOPIC,i,customer));
        }
        customerKafkaProducer.close();
    }

    public static void generateProductInput(){
        Properties properties=new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        KafkaProducer<Long,Product> productKafkaProducer=new KafkaProducer<Long, Product>(properties,Serdes.Long().serializer(),GlobalKtableUtil.getProductSerde().serializer());
        for (long i = 0; i <MAX_NUM ; i++) {
            Product product=new Product(randomString(10),randomString(MAX_NUM),randomString(20));
            productKafkaProducer.send(new ProducerRecord<Long, Product>(PRODUCT_TOPIC,i,product));
        }
        productKafkaProducer.close();
    }

    public static void receiveEnrichedOutput(){

        Properties properties=new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,GKT_CONSUMER_ID);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,Serdes.Long().deserializer());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,GlobalKtableUtil.getEnrichedOrderSerde().deserializer());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);

        KafkaConsumer<Long,EnrichedOrder> enrichedOrderKafkaConsumer=new KafkaConsumer<Long, EnrichedOrder>(properties,Serdes.Long().deserializer(),GlobalKtableUtil.getEnrichedOrderSerde().deserializer());
        enrichedOrderKafkaConsumer.subscribe(Collections.singleton(ENRICHED_ORDER_TOPIC));
        while(0<MAX_NUM) {
             enrichedOrderKafkaConsumer.poll(Long.MAX_VALUE)
                    .forEach(record -> System.out.println("key:" + record.key() + "Value:" + record.value()));
        }
    }
    private static String randomString(int len) {
        final StringBuilder b = new StringBuilder();
        for(int i = 0; i < len; ++i) {
            b.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".charAt(random.nextInt
                    ("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".length())));
        }
        return b.toString();
    }
}
