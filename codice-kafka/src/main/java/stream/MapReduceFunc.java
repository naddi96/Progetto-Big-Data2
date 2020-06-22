package stream;

import SerilizerDeserialazer.TimeExstractor;
import org.apache.kafka.streams.KeyValue;

import java.util.ArrayList;
import java.util.List;

public class MapReduceFunc {


    public static BussDelay reducers(BussDelay zb, BussDelay ab){
            ab.count=ab.count+zb.count;

            zb.Occurred_On= TimeExstractor.returnMinDate(zb.Occurred_On,ab.Occurred_On);
            zb.How_Long_Delayed=String.valueOf(Integer.valueOf(ab.How_Long_Delayed)+Integer.valueOf(zb.How_Long_Delayed));
            return zb;



    }



    public static List<KeyValue<String,BussDelay>> parseFlatmap(String x) {
        List<KeyValue<String,BussDelay>> kk=new ArrayList<KeyValue<String,BussDelay>>();
        BussDelay buss = new BussDelay(x);


            if (!buss.How_Long_Delayed.equals("")) {
                kk.add(new KeyValue<>(buss.Boro, buss));
            }
        return kk;
    }
}
