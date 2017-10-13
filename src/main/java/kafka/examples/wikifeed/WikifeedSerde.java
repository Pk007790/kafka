package kafka.examples.wikifeed;


import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by PravinKumar on 29/7/17.
 */
public class WikifeedSerde<T extends Serializable> implements Serde<T>{

    Serializer<T> serializer;
    Deserializer<T> deserializer;

    public static WikifeedSerde getInstance(){
        return new WikifeedSerde();
    }
    public WikifeedSerde(){
        serializer=new CustomSerializer();
        deserializer=new CustomDeserializer();
    }
    @Override
    public void configure(Map<String, ?> map, boolean b) {
        serializer.configure(map, b);
        deserializer.configure(map, b);
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }

    @Override
    public Serializer<T> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return deserializer;
    }
}