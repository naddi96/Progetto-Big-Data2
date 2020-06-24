package stream;

import SerilizerDeserialazer.TimeExstractor;
import org.apache.kafka.streams.KeyValue;

import java.util.*;

public class MapReduceFunc {


    public static BussDelay reducers(BussDelay zb, BussDelay ab){
            zb.count=ab.count+zb.count;
            zb.Occurred_On= TimeExstractor.returnMinDate(zb.Occurred_On,ab.Occurred_On);
            zb.How_Long_Delayed=String.valueOf(Integer.valueOf(ab.How_Long_Delayed)+Integer.valueOf(zb.How_Long_Delayed));
            zb.startingTimeNew = Math.max(zb.startingTimeNew,ab.startingTimeNew);
            zb.startingTimeOld = Math.min(zb.startingTimeOld,ab.startingTimeOld);
            return zb;

    }

    public static HashMap<String,Double> toMap(String a){
        HashMap<String, Double> map1 = new HashMap<>();

        for (String l :a.split(",")){
            String[] values = l.split(";");
            if (l.length()>0){
            map1.put(values[0],Double.valueOf(values[1]));
        }}
        return  map1;
    }


    public static BussDelay aggrecate(BussDelay zb, BussDelay ab){
        zb.startingTimeNew = Math.max(zb.startingTimeNew,ab.startingTimeNew);
        zb.startingTimeOld = Math.min(zb.startingTimeOld,ab.startingTimeOld);
        zb.Occurred_On=TimeExstractor.returnMinDate(zb.Occurred_On,ab.Occurred_On);
        zb.How_Long_Delayed.split(",");

        HashMap<String, Double> abMap = toMap(ab.How_Long_Delayed);
        HashMap<String, Double> zbMap = toMap(zb.How_Long_Delayed);

        Map<String, Double> map3 = new HashMap<>(zbMap);
        abMap.forEach((key,value)-> map3.merge(key,value,(v1,v2)-> Math.max(v1,v2)));

        Iterator it=map3.entrySet().iterator();
        String out="";
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            out= out+pair.getKey() + ";" + pair.getValue()+",";
            it.remove(); // avoids a ConcurrentModificationException
        }
        out=out.substring(0, out.length() - 1);

        zb.How_Long_Delayed=out;
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
