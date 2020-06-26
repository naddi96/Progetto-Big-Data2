package naddi.sadb.progetto.query2;

import naddi.sadb.progetto.KafkaConfig;
import naddi.sadb.progetto.utils.utils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Date;
import java.util.Properties;

public class Query2 {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment

        //Time time = Time.days(1);
        Time time = Time.days(7);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaConfig kafka=new KafkaConfig();
        Properties properties =kafka.getProperties();
        FlinkKafkaProducer<String> myProducer = kafka.getProducer();

        DataStream<String> text = env
                .addSource(new FlinkKafkaConsumer<>(kafka.input_topic, new SimpleStringSchema(), properties));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SingleOutputStreamOperator<Tuple2<String, BussDisservice>>
                stream = text.flatMap(new flatMapFascia());

        putEventTimeQuery2(stream, Time.days(1)).keyBy(0)
                    .window(TumblingEventTimeWindows.of(time))
                    .reduce((x, y) -> Query2MRFunc.reduceReason(x, y)).keyBy(0)
                    .map(new makeFasciaKey()).keyBy(0)
                    .window(TumblingEventTimeWindows.of(time))
                    .reduce((x, y) -> Query2MRFunc.reduceFascia(x, y)).keyBy(0)
                    .map(new get_top_3rank()).keyBy(0)
                    .window(TumblingEventTimeWindows.of(time))
                    .reduce((x,y)-> Query2MRFunc.reduceAll(x,y))
                    .map(new formatOutput())
                .addSink(myProducer);
                //  .print().setParallelism(1);


        env.execute("Query2");

    }



    public static SingleOutputStreamOperator<Tuple2<String,BussDisservice>>
    putEventTimeQuery2(DataStream<Tuple2<String,BussDisservice>> stream, Time time ){
        return stream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor
                        <Tuple2<String, BussDisservice>>(time) {
                    @Override
                    public long extractTimestamp(Tuple2<String,BussDisservice> tup) {
                        String format = "yyyy-MM-dd'T'HH:mm:ss.SSS";
                        String ex = tup.f1.Occurred_On;
                        Date dat = utils.parseDate(format,ex);
                        return dat.getTime();
                    }
                });
    }

}
