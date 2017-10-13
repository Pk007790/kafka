package kafka.examples.streams.wordcount;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by PravinKumar on 28/7/17.
 */
public  class  MySerde<T extends Serializable> implements Serde<T> {

    Serializer<T> serializer;
    Deserializer<T> deserializer;

    public MySerde() {
        serializer = new Serializer<T>() {
            @Override
            public void configure(Map<String, ?> map, boolean b) {
                // no-op
            }

            @Override
            public byte[] serialize(String topic, T data) {
                return SerializationUtils.serialize(data);
            }

            @Override
            public void close() {
                // no-op
            }
        };

        deserializer = new Deserializer<T>() {
            @Override
            public void configure(Map<String, ?> map, boolean b) {
                // no-op
            }

            @Override
            public T deserialize(String s, byte[] bytes) {
                return (T) SerializationUtils.deserialize(bytes);
            }

            @Override
            public void close() {
                // no-op
            }
        };
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
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