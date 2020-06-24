package naddi.sadb.progetto;


import naddi.sadb.progetto.query1Utils.BussDelay;
import naddi.sadb.progetto.query1Utils.utils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


class parseFlatMap extends RichFlatMapFunction<String, Tuple2<String, BussDelay>> {
    public void flatMap(String value, Collector<Tuple2<String,BussDelay>> out) {
                BussDelay c=new BussDelay(value);
                if (!c.How_Long_Delayed.equals("")  ){
                    out.collect(new Tuple2<>(c.Boro,c));
                }
            }
    }

class computeAvgMap extends RichMapFunction<Tuple2<String, BussDelay>, Tuple5<String, String, String,Long,Long>> {
    public Tuple5<String, String, String, Long,Long> map(Tuple2<String, BussDelay> tup) throws Exception {
        double avg=Double.valueOf(tup.f1.How_Long_Delayed) / Double.valueOf(tup.f1.count);
        return new Tuple5<String,String,String,Long,Long>(tup.f1.Boro,String.valueOf(avg),tup.f1.Occurred_On,tup.f1.startingTimeNew,tup.f1.startingTimeOld);
    }
}


class MapReduceFunctions {

    public static Tuple2<String, BussDelay> reduceBoro(Tuple2<String, BussDelay> tup1, Tuple2<String, BussDelay> tup2) throws Exception {
        //tup1.f1.Boro = tup1.f1.Boro + " " + tup2.f1.Boro;
        tup1.f1.count=tup1.f1.count+tup2.f1.count;
        tup1.f1.Occurred_On= utils.returnMinDate(tup1.f1.Occurred_On,tup2.f1.Occurred_On);
        tup1.f1.How_Long_Delayed=String.valueOf(Integer.valueOf(tup1.f1.How_Long_Delayed)+Integer.valueOf(tup2.f1.How_Long_Delayed));
        tup1.f1.startingTimeNew = Math.max(tup1.f1.startingTimeNew,tup2.f1.startingTimeNew);
        tup1.f1.startingTimeOld = Math.min(tup1.f1.startingTimeOld,tup2.f1.startingTimeOld);
        return new Tuple2<>(tup1.f0, tup1.f1);
    }


    public static SingleOutputStreamOperator<Tuple2<String,BussDelay>>
        putEventTime(DataStream<Tuple2<String,BussDelay>> stream, Time time ){
            return stream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor
                        <Tuple2<String, BussDelay>>(time) {
                    @Override
                    public long extractTimestamp(Tuple2<String,BussDelay> tup) {
                        String format = "yyyy-MM-dd'T'HH:mm:ss.SSS";
                        String ex = tup.f1.Occurred_On;
                        Date dat = utils.parseDate(format,ex);
                        return dat.getTime();//bussDelay.How_Long_Delayed;
                    }
                });
    }


}
