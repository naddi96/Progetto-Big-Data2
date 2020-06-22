package naddi.sadb.progetto;

import naddi.sadb.progetto.query1Utils.utils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Date;



class makeFasciaKey extends RichMapFunction<Tuple2<String,BussDisservice>,Tuple2<String,BussDisservice>> {
    @Override
    public Tuple2<String, BussDisservice> map(Tuple2<String, BussDisservice> tup) throws Exception {
        tup.f1.merge_fascia_list.add(new Tuple2<>(tup.f1.Reason,tup.f1.rank));
        return new Tuple2<>(tup.f1.fascia,tup.f1);
    }
}


class get_top_3rank extends RichMapFunction<Tuple2<String,BussDisservice>,String>{
    @Override
    public String map(Tuple2<String, BussDisservice> tup) throws Exception {
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
        return tup.f1.Occurred_On+","+tup.f1.fascia+","+out;
    }
}


class flatMapFascia  extends RichFlatMapFunction<String, Tuple2<String, BussDisservice>> {
    @Override
    public void flatMap(String line, Collector<Tuple2<String, BussDisservice>> collector) throws Exception {
        String[] x = line.split(";(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        String Occurred_On=x[7];
        String Reason=x[5];
        String format = "HH:mm:ss.SSS";
        String ex = Occurred_On.substring(11);
        Date data=utils.parseDate(format,ex);
        Date fascia5=utils.parseDate(format,"5:00:00.000");
        Date fascia11=utils.parseDate(format,"11:59:00.000");
        if ((data.after(fascia5)  && data.before(fascia11))
                || (data.equals(fascia5) || data.equals(fascia11))){

            collector.collect(new Tuple2<>("fascia5-11 "+Reason,new BussDisservice(Occurred_On,Reason,"fascia5-11") ));
        }
        Date fascia12=utils.parseDate(format,"12:00:00.000");
        Date fascia19=utils.parseDate(format,"19:00:00.000");
        if ((data.after(fascia12)  && data.before(fascia19))
                || (data.equals(fascia12) || data.equals(fascia19))){
            collector.collect(new Tuple2<>("fascia12-19 "+Reason,new BussDisservice(Occurred_On,Reason,"fascia12-19") ));
        }

    }

}

