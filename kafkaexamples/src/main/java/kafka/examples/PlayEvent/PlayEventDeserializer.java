package kafka.examples.PlayEvent;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Created by PravinKumar on 31/8/17.
 */
public class PlayEventDeserializer<T> implements Deserializer<T> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public T deserialize(String topic, byte[] data) {
        return (T) SerializationUtils.deserialize(data);
    }

    @Override
    public void close() {

    }
}
