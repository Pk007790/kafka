package kafka.examples.PlayEvent;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.UnlimitedWindows;

import java.util.Properties;

/**
 * Created by PravinKumar on 31/8/17.
 */
public class PlayEventExample {

    private static final String PLAY_EVENT_ID="playeventid";
    public static final String BOOTSTRAP_SERVER="localhost:9092";
    private static final String STAT_DIR="/home/admin/Documents/kafka_2.11-0.10.2.1/kafka-streams";
    public static final String PLAY_EVENT_TOPIC="playeventtopicu13";
    private static final String PLAY_EVENT_STORE="playeventstore";
    public static final String PLAY_EVENT_OUTPUT="playeventoutputtopicu13";
    public static final Long START_TIME=System.currentTimeMillis();
    //public static final Long INACTIVITY_GAP = TimeUnit.MINUTES.toMillis(30);
    //public static final long INACTIVITY_GAP_SIZE=TimeUnit.MINUTES.toMillis(10);
    //public static final long INACTIVITY_ADVANCE_SIZE=TimeUnit.MINUTES.toMillis(5);


    public static void main(String[] args) {
        KafkaStreams kafkaStreams =getStreams();
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    public static KafkaStreams getStreams(){
        Properties props=new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,PLAY_EVENT_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,PlayEventSerde.class);
        props.put(StreamsConfig.STATE_DIR_CONFIG,STAT_DIR);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KStreamBuilder builder=new KStreamBuilder();
        PlayEventSerde<PlayEvent> playEventSerde=new PlayEventSerde<>();
        builder.stream(Serdes.String(),playEventSerde,PLAY_EVENT_TOPIC)
                .groupByKey(Serdes.String(),playEventSerde)
                .count(UnlimitedWindows.of().startOn(START_TIME),PLAY_EVENT_STORE)
                .toStream()
                .map(((key, value) -> KeyValue.pair(key.key()+"@"+key.window().start()+"->"+key.window().end(),value)))
                .to(Serdes.String(),Serdes.Long(),PLAY_EVENT_OUTPUT);

        KafkaStreams streams=new KafkaStreams(builder,props);
        return streams;

        // a. Session Window
        //
        // sizeMs and advanceMs
        // b. Tumbling Window
        // c. Hopping Time Window
        //
        // d. Unlimited time windows
    }
}
