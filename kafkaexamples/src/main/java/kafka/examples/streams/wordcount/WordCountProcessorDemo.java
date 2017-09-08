package kafka.examples.streams.wordcount;

/**
 * Created by PravinKumar on 12/7/17.
 */
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.util.Locale;
import java.util.Properties;
/**
 * Demonstrates, using the low-level Processor APIs, how to implement the WordCount program
 * that computes a simple word occurrence histogram from an input text.
 *
 * In this example, the input stream reads from a topic named "streams-file-input", where the values of messages
 * represent lines of text; and the histogram output is written to topic "streams-wordcount-processor-output" where each record
 * is an updated count of a single word.
 *
 * Before running this example you must create the source topic (e.g. via bin/kafka-topics.sh --create ...)
 * and write some data to it (e.g. via bin-kafka-console-producer.sh). Otherwise you won't see any data arriving in the output topic.
 */
public class WordCountProcessorDemo {
    private static class MyProcessorSupplier implements ProcessorSupplier<String,String> {
        @Override
        public Processor<String, String> get() {
            return new Processor<String, String>() {
                private ProcessorContext context;
                private KeyValueStore<String, Integer> kvStore;

                @Override
                @SuppressWarnings("unchecked")
                public void init(ProcessorContext context) {
                    this.context = context;
                    this.context.schedule(1000);
                    this.kvStore = (KeyValueStore<String, Integer>) context.getStateStore("Counts");
                }

                @Override
                public void process(String dummy, String line) {
                    System.out.println("Val : " + line + ", offset : " + context.offset());
                    String[] words = line.toLowerCase(Locale.getDefault()).split(" ");
                    for (String word : words) {
                        Integer oldValue = this.kvStore.get(word);
                        if (oldValue == null) {
                            this.kvStore.put(word, 1);
                        } else {
                            this.kvStore.put(word, oldValue + 1);
                        }
                    }
                    context.commit();
                }

                @Override
                public void punctuate(long timestamp) {
                    try (KeyValueIterator<String, Integer> iter = this.kvStore.all()) {
                        System.out.println("----------- " + timestamp + " ----------- ");
                        while (iter.hasNext()) {
                            KeyValue<String, Integer> entry = iter.next();
                            System.out.println("[" + entry.key + ", " + entry.value + "]");
                            context.forward(entry.key, entry.value.toString());
                        }
                    }
                }
                @Override
                public void close() {
                    this.kvStore.close();
                }
            };
        }
    }
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/pravin-state");

        // setting offset reset to earliest so that we can re-run the demo code with
        // the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("Source", "streams-file-input");
        builder.addProcessor("Process", new MyProcessorSupplier(), "Source");
        builder.addStateStore(Stores.create("Counts").withStringKeys().withIntegerValues().inMemory().build(), "Process");
        builder.addSink("Sink", "streams-wordcount-processor-output", "Process");


        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
// usually the stream application would be running forever,
// in this example we just let it run for some time and stop since the input data is finite.
        Thread.sleep(5000L);
        streams.close();
    }
}