package naddi.sadb.progetto.query2;

import naddi.sadb.progetto.utils.utils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


class Query2MRFunc{

    public static Tuple2<String, BussDisservice>
    reduceAll
            (Tuple2<String, BussDisservice> tup1, Tuple2<String, BussDisservice> tup2) throws Exception {

        String rank_mattino="";
        String rank_pom="";
        if(tup1.f1.fascia.equals("fascia5-11")){
             rank_mattino= tup1.f1.Reason;
        }

        if(tup1.f1.fascia.equals("fascia12-19")){
             rank_pom= tup1.f1.Reason;

        }

        if(tup2.f1.fascia.equals("fascia5-11")){
             rank_mattino= tup2.f1.Reason;
        }

        if(tup2.f1.fascia.equals("fascia12-19")){
             rank_pom= tup2.f1.Reason;
        }

        tup1.f1.startingTimeNew = Math.max(tup1.f1.startingTimeNew,tup2.f1.startingTimeNew);
        tup1.f1.startingTimeOld = Math.min(tup1.f1.startingTimeOld,tup2.f1.startingTimeOld);
        tup1.f1.Occurred_On=utils.returnMinDate(tup1.f1.Occurred_On,tup2.f1.Occurred_On);

        tup1.f1.Reason="fascia5-11,"+rank_mattino+",fascia12-19,"+rank_pom;
        return new Tuple2<>(tup1.f0, tup1.f1);
    }

    public static Tuple2<String, BussDisservice>
    reduceFascia
            (Tuple2<String, BussDisservice> tup1, Tuple2<String, BussDisservice> tup2) throws Exception {

        tup1.f1.startingTimeNew = Math.max(tup1.f1.startingTimeNew,tup2.f1.startingTimeNew);
        tup1.f1.startingTimeOld = Math.min(tup1.f1.startingTimeOld,tup2.f1.startingTimeOld);
        tup1.f1.merge_fascia_list.addAll(tup2.f1.merge_fascia_list);
        tup1.f1.Occurred_On=utils.returnMinDate(tup1.f1.Occurred_On,tup2.f1.Occurred_On);
        return new Tuple2<>(tup1.f0, tup1.f1);
    }

    public static Tuple2<String, BussDisservice>
    reduceReason
            (Tuple2<String, BussDisservice> tup1, Tuple2<String, BussDisservice> tup2) throws Exception {
        //tup1.f1.Boro = tup1.f1.Boro + " " + tup2.f1.Boro;
        tup1.f1.rank=tup1.f1.rank+tup2.f1.rank;
        tup1.f1.startingTimeNew = Math.max(tup1.f1.startingTimeNew,tup2.f1.startingTimeNew);
        tup1.f1.startingTimeOld = Math.min(tup1.f1.startingTimeOld,tup2.f1.startingTimeOld);
        tup1.f1.Occurred_On=utils.returnMinDate(tup1.f1.Occurred_On,tup2.f1.Occurred_On);
        return new Tuple2<>(tup1.f0, tup1.f1);
    }

}


class makeFasciaKey extends RichMapFunction<Tuple2<String,BussDisservice>,Tuple2<String,BussDisservice>> {
    @Override
    public Tuple2<String, BussDisservice> map(Tuple2<String, BussDisservice> tup) throws Exception {
        tup.f1.merge_fascia_list.add(new Tuple2<>(tup.f1.Reason,tup.f1.rank));
        return new Tuple2<>(tup.f1.fascia,tup.f1);
    }
}


class formatOutput extends RichMapFunction<Tuple2<String,BussDisservice>,String> {
    @Override
    public String map(Tuple2<String, BussDisservice> tup) throws Exception {
        String startingtime=tup.f1.startingTimeNew+","+tup.f1.startingTimeOld+",";
        if(!tup.f1.Reason.contains("fascia")){
            if(tup.f1.fascia.equals("fascia5-11"))
                return startingtime+tup.f1.Occurred_On+",fascia5-11,"+tup.f1.Reason+",fascia12-19,";
            if(tup.f1.fascia.equals("fascia12-19"))
                return startingtime+tup.f1.Occurred_On+",fascia5-11,,fascia12-19,"+tup.f1.Reason;
        }
        return startingtime+tup.f1.Occurred_On+","+tup.f1.Reason;
    }
}



class get_top_3rank extends RichMapFunction<Tuple2<String,BussDisservice>,Tuple2<String,BussDisservice>>{
    @Override
    public Tuple2<String,BussDisservice> map(Tuple2<String, BussDisservice> tup) throws Exception {
        int len =tup.f1.merge_fascia_list.size();
        String out="";
        if (len>0){
            Tuple2<String,Integer>max1= utils.exstractMax(tup.f1.merge_fascia_list);
            tup.f1.merge_fascia_list.remove(max1);
            out= max1.f0+":"+max1.f1;
        }
        if (len>1){
            Tuple2<String,Integer>max2=utils.exstractMax(tup.f1.merge_fascia_list);
            tup.f1.merge_fascia_list.remove(max2);
            out=out+"/"+ max2.f0+":"+max2.f1;
        }
        if (len>2){
            Tuple2<String,Integer>max3=utils.exstractMax(tup.f1.merge_fascia_list);
            tup.f1.merge_fascia_list.remove(max3);
            out=out+"/"+ max3.f0+":"+max3.f1;

        }

        tup.f1.Reason=out;
        return new Tuple2<>("x",tup.f1);
    }
}


class flatMapFascia  extends RichFlatMapFunction<String, Tuple2<String, BussDisservice>> {
    @Override
    public void flatMap(String line, Collector<Tuple2<String, BussDisservice>> collector) throws Exception {
        BussDisservice buss=new BussDisservice(line);
        if (buss.fascia.equals("fascia5-11")) {
            collector.collect(new Tuple2<>("fascia5-11 "+buss.Reason,buss));
        }
        if (buss.fascia.equals("fascia12-19")){
            collector.collect(new Tuple2<>("fascia12-19 "+buss.Reason,buss ));
        }

    }

}

