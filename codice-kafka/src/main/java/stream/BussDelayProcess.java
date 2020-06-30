package stream;

import SerilizerDeserialazer.DeserialazerBuss;
import SerilizerDeserialazer.SerializerBuss;
import SerilizerDeserialazer.TimeExstractor;
import config.Configuration;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

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

//  $KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic streams
    private static Properties createStreamProperties(){
        final Properties props = new Properties();

        // Give the Streams application a unique name.
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "AVGcompute");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "lambda-client");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.BOOTSTRAP_SERVERS);

        // Specify default (de)serializers for record keys and for record values.
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());


        props//.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
                .put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        // props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
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
                builder.stream("input-stream");


      int durata=1;
       Duration durataWindow = Duration.ofDays(durata);


       // int durata=7;
        //Duration durataWindow = Duration.ofDays(durata);


        //int durata=30;
        //Duration durataWindow = Duration.ofDays(durata);


        KStream<String, BussDelay> buss = textLines.flatMap((key, value) -> MapReduceFunc.parseFlatmap(value));
        TimeWindowedKStream<String, BussDelay> widowedBuss = buss.groupByKey(Serialized.with(Serdes.String(),BussDelaySerde))
                .windowedBy(TimeWindows.of(durataWindow));

        KTable<Windowed<String>, BussDelay> reducedBuss = widowedBuss.reduce((z, a) -> MapReduceFunc.reducers(z, a));
        KStream<Windowed<String>, BussDelay> avgBuss = reducedBuss.mapValues(value -> MapReduceFunc.calcolaAVG(value)).toStream();

        KGroupedStream<String, BussDelay> avg =
                avgBuss.groupBy((key, value) ->TimeExstractor.windowsTime(value.Occurred_On,durata), Serialized.with(Serdes.String(), BussDelaySerde));
        KTable<String, BussDelay> agrecated = avg.reduce((i,j)-> MapReduceFunc.aggrecate(i,j));
        KTable<String, String> output = agrecated.mapValues(value
                -> value.startingTimeNew+","+value.startingTimeOld+","+value.Occurred_On+","+value.How_Long_Delayed);
        output.toStream().to("output-stream",Produced.with(Serdes.String(), Serdes.String()));
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        streams.start();
        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


}

