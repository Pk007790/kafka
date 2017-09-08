package kafka.examples.PageViewRegion;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by PravinKumar on 7/9/17.
 */
public class MySerde<T extends Serializable> implements Serde<T> {


    Serializer pageviewSerializer;
    Deserializer pageviewDeserializer;

    public static MySerde getSerde(){
        return new MySerde();
    }
    public MySerde(){
        pageviewSerializer=new CustomSerializer<>();
        pageviewDeserializer=new CustomDeserializer<>();
    }
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        pageviewSerializer.configure(configs, isKey);
        pageviewDeserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        pageviewSerializer.close();
        pageviewDeserializer.close();
    }

    @Override
    public Serializer<T> serializer() {
        return pageviewSerializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return pageviewDeserializer;
    }
}
