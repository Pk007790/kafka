package kafka.examples.PageViewRegion;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Created by PravinKumar on 7/9/17.
 */
public class CustomDeserializer<T> implements Deserializer<T> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public T deserialize(String topic, byte[] data) {

        if (data==null)return null;
        return (T) SerializationUtils.deserialize(data);
    }

    @Override
    public void close() {

    }
}
