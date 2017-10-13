package kafka.examples.GlobalKTable;

public class GlobalKtableUtil {



    public static GlobalKTableSerde<Order> getOrderSerde() {
        return  GlobalKTableSerde.getInstance();
    }

    public static GlobalKTableSerde<Customer> getCustomerSerde() {
        return GlobalKTableSerde.getInstance();
    }

    public static GlobalKTableSerde<Product> getProductSerde() {
        return GlobalKTableSerde.getInstance();
    }

    public static GlobalKTableSerde<EnrichedOrder> getEnrichedOrderSerde() {
        return GlobalKTableSerde.getInstance();
    }
}
