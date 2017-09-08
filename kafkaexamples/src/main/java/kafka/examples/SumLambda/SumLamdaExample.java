package kafka.examples.SumLambda;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

/**
 * Created by PravinKumar on 18/8/17.
 */
public class SumLamdaExample {

    public static final String BOOTSTRAP_SERVER = "localhost:9092";
    public static final String SUM_INPUT_EVEN_TOPIC = "suminputeventopicy";
    public static final String SUM_INPUT_ODD_TOPIC = "suminputoddtopicy";

    public static final String SUM_OUTPUT_EVEN_TOPIC = "sumoutputeventopicy";
    public static final String SUM_OUTPUT_ODD_TOPIC = "sumoutputoddtopicy";

    private static final String SUMLAMBDA_ID= "sumlambdaId";
    private static final String STAT_DIR="/home/admin/Documents/kafka_2.11-0.10.2.1/kafka-streams";


    public static void main(String[] args) {
        KafkaStreams kafkaStreams=getSumStreams();
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    public static KafkaStreams getSumStreams(){

        Properties props=new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,SUMLAMBDA_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        props.put(StreamsConfig.STATE_DIR_CONFIG,STAT_DIR);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,10*1000);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.Integer().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        KStreamBuilder builder=new KStreamBuilder();
        KStream<Integer,Integer> suminput=builder.stream(SUM_INPUT_EVEN_TOPIC);
        getKTableForEvenNums(suminput).to(SUM_OUTPUT_EVEN_TOPIC);

        KStream<Integer,Integer> oddinput=builder.stream(SUM_INPUT_ODD_TOPIC);
        getKTableForOddNums(oddinput).to(SUM_OUTPUT_ODD_TOPIC);
        KafkaStreams kafkaStreams=new KafkaStreams(builder,props);

        return kafkaStreams;
    }

    private static KTable getKTableForOddNums(KStream<Integer,Integer> sumoddinput){
        KTable<Integer,Integer> koddTable=sumoddinput
                .filter((key,value)-> value%2 !=0)
                .selectKey((key, value) -> 1)
                .groupByKey()
                .reduce((v1, v2)-> v1 + v2,"oddsumy");
        return koddTable;
    }

    private static KTable getKTableForEvenNums(KStream<Integer,Integer> sumeveninput){
        KTable<Integer,Integer> kTable=sumeveninput
                .filter((key,value)-> value%2 ==0)
                .selectKey((key, value) -> 1)
                .groupByKey()
                .reduce((v1, v2)-> v1 + v2,"sumy");
        return kTable;
    }
}
