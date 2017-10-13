package kafka.examples.GlobalKTable;

/**
 * Created by PravinKumar on 8/8/17.
 */
public class CustomerOrder {


    public static Order order;
    public static Customer customer;

    public static CustomerOrder getInstance(){
        return new CustomerOrder();
    }

    public CustomerOrder() {
    }

    public CustomerOrder(Order order, Customer customer) {
        this.order = order;
        this.customer = customer;
    }

    public static Order getOrder() {
        return order;
    }

    public static void setOrder(Order order) {
        CustomerOrder.order = order;
    }

    public static Customer getCustomer() {
        return customer;
    }

    public static void setCustomer(Customer customer) {
        CustomerOrder.customer = customer;
    }

    public static long  productId() {
       return order.getProduct_id();
    }

    @Override
    public String toString() {
        return "CustomerOrder{" +
                "order=" + order +
                ", customer=" + customer +
                '}';
    }
}
