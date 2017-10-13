package kafka.examples.GlobalKTable;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by PravinKumar on 4/8/17.
 */
public class GlobalCustomSerializer<T> implements Serializer<T> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, T t) {
        return  SerializationUtils.serialize((Serializable) t);
    }

    @Override
    public void close() {

    }
}
