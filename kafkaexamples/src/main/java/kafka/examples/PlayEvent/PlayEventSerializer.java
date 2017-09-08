package kafka.examples.PlayEvent;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by PravinKumar on 31/8/17.
 */
public class PlayEventSerializer<T> implements Serializer<T> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, T data) {
        return SerializationUtils.serialize((Serializable) data);
    }

    @Override
    public void close() {

    }
}
