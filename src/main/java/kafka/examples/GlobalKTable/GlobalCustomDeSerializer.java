package kafka.examples.GlobalKTable;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Created by PravinKumar on 4/8/17.
 */
public class GlobalCustomDeSerializer<T> implements Deserializer<T> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        return (T) SerializationUtils.deserialize(bytes);
    }

    @Override
    public void close() {

    }
}
