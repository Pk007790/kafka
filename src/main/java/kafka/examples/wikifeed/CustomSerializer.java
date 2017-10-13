package kafka.examples.wikifeed;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by PravinKumar on 29/7/17.
 */
public class CustomSerializer<T> implements Serializer<T> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, T t) {
        return SerializationUtils.serialize((Serializable) t);
    }

    @Override
    public void close() {

    }
}
