package kafka.examples.PageViewRegion;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

/**
 * Created by PravinKumar on 7/9/17.
 */
public class PageViewRegionLambdaExample {

    private static final String PAGE_VIEW_REGION="pageviewregionx";
    public static final String BOOTSTRAP_SERVER ="localhost:9092";
    public static final String USER_PROFILE="userprofile";
    private static final String USER_PROFILE_QSSTORE="userprofilestorequerystore";
    private static final String VIEW_REGION_STORE="viewregionstore";
    public static final String VIEW_REGION="viewregion";
    public static final String PAGE_VIEW="pageview";
    public static final String STAT_DIR="/home/admin/Documents/kafka_2.11-0.10.2.1/kafka-streams";


    public static void main(String[] args) {

        KafkaStreams streams=getStreams();
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static Properties getProps(){

        Properties props=new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,PAGE_VIEW_REGION);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,MySerde.class);
        props.put(StreamsConfig.STATE_DIR_CONFIG,STAT_DIR);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        return props;
    }

    public static KafkaStreams getStreams(){
        Properties props=getProps();
        KStreamBuilder builder=new KStreamBuilder();
        KStream<String,PageView> pageViewKStream=builder.<String,PageView>stream(PAGE_VIEW)
                .map(((key, pageView) -> new KeyValue<>(pageView.getName(), pageView)));
        KTable<String,UserProfile> userProfileKTable=builder.table(USER_PROFILE,USER_PROFILE_QSSTORE);
        KTable<Windowed<String>,Long> viewRegion=pageViewKStream.leftJoin(userProfileKTable,(view,userProfile)->{
        ViewRegion viewregion=new ViewRegion();
        viewregion.setUser(view.getName());
        viewregion.setPage(view.getPage());
        viewregion.setRegion(userProfile.getRegion());

        return viewregion;
        }).map(((username, region) -> new KeyValue<>(region.getRegion(),region)))
                .groupByKey()
                .count(TimeWindows.of(5*60*1000L).advanceBy(60*1000L),VIEW_REGION_STORE);

        KStream<String,Long> viewRegionStream=viewRegion.toStream((Windowed,Long)-> Windowed.toString());
        viewRegionStream.to(Serdes.String(),Serdes.Long(),VIEW_REGION);
        KafkaStreams kafkaStreams=new KafkaStreams(builder,props);

        return kafkaStreams;
    }
}
