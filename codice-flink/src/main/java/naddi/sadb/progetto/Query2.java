package naddi.sadb.progetto;

import naddi.sadb.progetto.BussDisservice;
import naddi.sadb.progetto.query1Utils.utils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

public class Query2 {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment

        final String hostname = "127.0.0.1";
        final int port = 444;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //CPU_CORES= new ConfigOption("12");

        DataStream<String> text = env.socketTextStream(hostname, port, "\n");
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SingleOutputStreamOperator<Tuple2<String, BussDisservice>>
                stream = text.flatMap(new flatMapFascia());
                    putEventTimeQuery2(stream, Time.days(1)).keyBy(0)
                    .window(TumblingEventTimeWindows.of(Time.hours(24)))
                    .reduce((x, y) -> reduceReason(x, y)).keyBy(0)
                    .map(new makeFasciaKey()).keyBy(0)
                    .window(TumblingEventTimeWindows.of(Time.hours(24)))
                    .reduce((x, y) -> reduceFascia(x, y)).keyBy(0)
                    .map(new get_top_3rank())
                .print().setParallelism(1);
        env.execute("Socket Window WordCount");

    }

    public static Tuple2<String, BussDisservice>
    reduceFascia
            (Tuple2<String, BussDisservice> tup1, Tuple2<String, BussDisservice> tup2) throws Exception {

        tup1.f1.merge_fascia_list.addAll(tup2.f1.merge_fascia_list);

        return new Tuple2<>(tup1.f0, tup1.f1);
    }

    public static Tuple2<String, BussDisservice>
    reduceReason
            (Tuple2<String, BussDisservice> tup1, Tuple2<String, BussDisservice> tup2) throws Exception {
        //tup1.f1.Boro = tup1.f1.Boro + " " + tup2.f1.Boro;
        tup1.f1.rank=tup1.f1.rank+tup2.f1.rank;
        tup1.f1.Occurred_On=utils.returnMinDate(tup1.f1.Occurred_On,tup2.f1.Occurred_On);
        return new Tuple2<>(tup1.f0, tup1.f1);
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
