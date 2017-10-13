package kafka.examples.PlayEvent;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by PravinKumar on 31/8/17.
 */
public class PlayEventSerde<T extends Serializable> implements Serde<T>{

    Serializer<T> playEventSerializer;
    Deserializer<T> playEventDeserializer;

    public static PlayEventSerde getInstance(){
        return new PlayEventSerde();
    }

    public PlayEventSerde(){
        playEventSerializer=new PlayEventSerializer();
        playEventDeserializer=new PlayEventDeserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        playEventSerializer.configure(configs, isKey);
        playEventDeserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        playEventSerializer.close();
        playEventDeserializer.close();
    }

    @Override
    public Serializer<T> serializer() {
        return playEventSerializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return playEventDeserializer;
    }
}