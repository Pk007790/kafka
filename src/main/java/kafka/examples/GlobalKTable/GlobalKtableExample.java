package kafka.examples.GlobalKTable;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;



public class GlobalKtableExample {

    private static final String ORDER_TOPIC = "ordertopic";
    private static final String CUSTOMER_TOPIC="customertopic";
    private static final String PRODUCT_TOPIC="producttopic";
    private static final String CUSTOMER_STORE="customerstore";
    private static final String PRODUCT_STORE="productstore";
    private static final String ENRICHED_ORDER_TOPIC="enrichedordertopic";
    private static final String GLOBAL_KTABLE="globalktableexample";
    private static final String BOOTSTRAP_SERVER="localhost:9092";
    private static final String STAT_DIR="/home/admin/Documents/kafka_2.11-0.10.2.1/kafka-streams";

    public static void main(String[] args) {
        KafkaStreams kafkaStreams=getKafkaStreams();
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    public static KafkaStreams getKafkaStreams(){
        Properties properties=new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,GLOBAL_KTABLE);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        properties.put(StreamsConfig.STATE_DIR_CONFIG,STAT_DIR);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,500);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KStreamBuilder builder=new KStreamBuilder();

        KStream<Long,Order> orderKStream=builder.stream(Serdes.Long(),GlobalKtableUtil.getOrderSerde(),ORDER_TOPIC);

        GlobalKTable<Long,Customer> customerGlobalKTable=builder.globalTable(Serdes.Long(),GlobalKtableUtil.getCustomerSerde(),CUSTOMER_TOPIC,CUSTOMER_STORE);
        GlobalKTable<Long,Product> productGlobalKTable=builder.globalTable(Serdes.Long(),GlobalKtableUtil.getProductSerde(),PRODUCT_TOPIC,PRODUCT_STORE);

        KStream<Long,CustomerOrder> customerOrderKStream=orderKStream.join(customerGlobalKTable,
                (orderId,Order)-> Order.getCustomer_id(),
        (Order,Customer) -> new CustomerOrder(Order,Customer));

        KStream<Long,EnrichedOrder> enrichedOrderKStream=customerOrderKStream.join(productGlobalKTable,
                (orderId,CustomerOrder)-> CustomerOrder.productId(),
                (CustomerOrder,Product)-> new EnrichedOrder(CustomerOrder.order, kafka.examples.GlobalKTable.CustomerOrder.getInstance().customer,Product));
                enrichedOrderKStream.to(Serdes.Long(),GlobalKtableUtil.getEnrichedOrderSerde(),ENRICHED_ORDER_TOPIC);
        KafkaStreams kafkaStreams=new KafkaStreams(builder,properties);

        return kafkaStreams;
    }

}
