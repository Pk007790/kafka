package kafka.examples.GlobalKTable;

import java.io.Serializable;

/**
 * Created by PravinKumar on 4/8/17.
 */
public class Order implements Serializable{

    public  long customer_id;
    public  long product_id;
    public  long order_placed_time;

    public Order() {
    }

    public Order(long customer_id, long product_id, long order_placed_time) {
        this.customer_id = customer_id;
        this.product_id = product_id;
        this.order_placed_time = order_placed_time;
    }


    public long getCustomer_id() {
        return customer_id;
    }

    public void setCustomer_id(long customer_id) {
        this.customer_id = customer_id;
    }

    public long getProduct_id() {
        return product_id;
    }

    public void setProduct_id(long product_id) {
        this.product_id = product_id;
    }

    public long getOrder_placed_time() {
        return order_placed_time;
    }

    public void setOrder_placed_time(long order_placed_time) {
        this.order_placed_time = order_placed_time;
    }

    @Override
    public String toString() {
        return "Order{" +
                "customer_id=" + customer_id +
                ", product_id=" + product_id +
                ", order_placed_time=" + order_placed_time +
                '}';
    }
}
