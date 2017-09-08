package kafka.examples.GlobalKTable;

import java.io.Serializable;

/**
 * Created by PravinKumar on 4/8/17.
 */
public class Customer implements Serializable{

    private  String name;
    private  String gender;
    private  String region;


    public Customer() {
    }

    public Customer(String name, String gender, String region) {
        this.name = name;
        this.gender = gender;
        this.region = region;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    @Override
    public String toString() {
        return "Customer{" +
                "name='" + name + '\'' +
                ", gender='" + gender + '\'' +
                ", region='" + region + '\'' +
                '}';
    }
}
