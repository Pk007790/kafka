package kafka.examples.GlobalKTable;

import java.io.Serializable;

/**
 * Created by PravinKumar on 4/8/17.
 */
public class EnrichedOrder implements Serializable {

    private  Order order;
    private  Customer customer;
    private  Product product;

    public EnrichedOrder() {
    }

    public static EnrichedOrder getInstance(){
        return new EnrichedOrder();
    }

    public EnrichedOrder(Order order, Customer customer, Product product) {
        this.order = order;
        this.customer = customer;
        this.product = product;
    }

    public Order getOrder() {
        return order;
    }

    public void setOrder(Order order) {
        this.order = order;
    }

    public Customer getCustomer() {
        return customer;
    }

    public void setCustomer(Customer customer) {
        this.customer = customer;
    }

    public Product getProduct() {
        return product;
    }

    public void setProduct(Product product) {
        this.product = product;
    }

    @Override
    public String toString() {
        return "EnrichedOrder{" +
                "order=" + order +
                ", customer=" + customer +
                ", product=" + product +
                '}';
    }
}
