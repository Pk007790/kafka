package kafka.examples.GlobalKTable;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by PravinKumar on 4/8/17.
 */
public class GlobalKTableSerde<T extends Serializable> implements Serde<T>{

    Serializer<T> serializer;
    Deserializer<T>deserializer;

    public static GlobalKTableSerde getInstance(){
        return new GlobalKTableSerde();
    }

    GlobalKTableSerde(){
        serializer=new GlobalCustomSerializer<>();
        deserializer=new GlobalCustomDeSerializer<>();
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
