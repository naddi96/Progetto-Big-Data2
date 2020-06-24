package stream;

import SerilizerDeserialazer.DeserialazerBuss;
import SerilizerDeserialazer.SerializerBuss;
import SerilizerDeserialazer.TimeExstractor;
import config.Configuration;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;


public class FinalOutput {


    private static Properties createStreamProperties(){
        final Properties props = new Properties();

        // Give the Streams application a unique name.
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "finalout");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.BOOTSTRAP_SERVERS);

        // Specify default (de)serializers for record keys and for record values.
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());

        // Records should be flushed every 10 seconds.
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        return props;
    }


    public static void start(){
        SerializerBuss ser= new SerializerBuss();
        DeserialazerBuss des =new DeserialazerBuss();
        final Serde<BussDelay> BussDelaySerde = Serdes.serdeFrom(ser, des);
        final Properties props = createStreamProperties();
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TimeExstractor.class);
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, BussDelay> textLines =
                builder.stream("streams-wordcount-output");




    }

}
