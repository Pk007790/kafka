package kafka.examples.GlobalKTable;

import java.io.Serializable;

/**
 * Created by PravinKumar on 4/8/17.
 */
public class Product implements Serializable{

    private  String name;
    private  String description;
    private  String supplierName;

    public static Product getInstance(){
        return new Product();
    }

    public Product() {
    }

    public Product(String name, String description, String supplierName) {
        this.name = name;
        this.description = description;
        this.supplierName = supplierName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getSupplierName() {
        return supplierName;
    }

    public void setSupplierName(String supplierName) {
        this.supplierName = supplierName;
    }

    @Override
    public String toString() {
        return "Product{" +
                "name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", supplierName='" + supplierName + '\'' +
                '}';
    }
}
