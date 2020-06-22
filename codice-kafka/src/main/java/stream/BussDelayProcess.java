package stream;

import SerilizerDeserialazer.DeserialazerBuss;
import SerilizerDeserialazer.SerializerBuss;
import SerilizerDeserialazer.TimeExstractor;
import config.Configuration;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.*;

/*
 * 1) Create the input and output topics used by this example.
 * $ bin/kafka-topics.sh --create --topic streams-plaintext-input \
 *                       --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * $ bin/kafka-topics.sh  --create --topic streams-wordcount-output \
 *                        --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 *
 * 2) Start this  application
 *
 * 3) Write some input data to the source topic "streams-plaintext-input"
 *      e.g.:
 * $ bin/kafka-console-producer --broker-list localhost:9092 --topic streams-plaintext-input
 *
 * 4) Inspect the resulting data in the output topic "streams-wordcount-output"
 *      e.g.:
 * $ bin/kafka-console-consumer --topic streams-wordcount-output --from-beginning \
 *                              --new-consumer --bootstrap-server localhost:9092 \
 *                              --property print.key=true \
 *                              --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 */
public class BussDelayProcess {


    private static Properties createStreamProperties(){
        final Properties props = new Properties();

        // Give the Streams application a unique name.
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "wordcount-lambda-example-client");
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




    public static void main(final String[] args) throws Exception {

        SerializerBuss ser= new SerializerBuss();
        DeserialazerBuss des =new DeserialazerBuss();
        final Serde<BussDelay> BussDelaySerde = Serdes.serdeFrom(ser, des);

        final Properties props = createStreamProperties();
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TimeExstractor.class);
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> textLines =
                builder.stream("streams");




        KStream<String, BussDelay> buss = textLines.flatMap((key, value) -> MapReduceFunc.parseFlatmap(value));


        TimeWindowedKStream<String, BussDelay> widowedBuss = buss.groupByKey(Serialized.with(Serdes.String(),BussDelaySerde))
                .windowedBy(TimeWindows.of(Duration.ofDays(1)));

        KTable<Windowed<String>, BussDelay> reducedBuss = widowedBuss.reduce((z, a) -> MapReduceFunc.reducers(z, a));
        KTable<Windowed<String>, String> tostringBuss = reducedBuss.mapValues(value -> {
                String ret = value.Occurred_On;
                double avg = Double.valueOf(value.How_Long_Delayed) / Double.valueOf(value.count);
                ret = ret + "," + value.Boro + "," + avg;
            System.out.println(ret);
            return ret;
        });

        KStream<String, String> toStreamBuss = tostringBuss.toStream().map((key, value) -> KeyValue.pair(key.key(), value));
        toStreamBuss.to("streams-wordcount-output",Produced.with(Serdes.String(), Serdes.String()));
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}

