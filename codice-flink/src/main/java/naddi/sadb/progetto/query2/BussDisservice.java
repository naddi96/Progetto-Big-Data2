package naddi.sadb.progetto.query2;

import naddi.sadb.progetto.utils.utils;
import org.apache.flink.api.java.tuple.Tuple2;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class BussDisservice {
    public String Occurred_On;
    public String Reason;
    public int rank=1;
    public    Long startingTimeNew;
    public    Long startingTimeOld;
    public String fascia="";
    public List<Tuple2<String, Integer>> merge_fascia_list= new ArrayList<Tuple2<String, Integer>>();
    public BussDisservice() {}



    @Override
    public String toString() {
        return "BussDisservice{" +
                "Occurred_On='" + Occurred_On + '\'' +
                ", Reason='" + Reason + '\'' +
                ", rank=" + rank +
                ", fascia='" + fascia + '\'' +
                ", merge_fascia_list=" + merge_fascia_list +
                '}';
    }


    public BussDisservice(String line) {
        String[] x = line.split(";(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        this.startingTimeNew=Long.valueOf(x[0]);
        this.startingTimeOld=Long.valueOf(x[0]);
        this.Occurred_On=x[7+1];
        this.Reason=x[5+1];
        String format = "HH:mm:ss.SSS";
        String ex = Occurred_On.substring(11);
        Date data= utils.parseDate(format,ex);
        Date fascia5=utils.parseDate(format,"5:00:00.000");
        Date fascia11=utils.parseDate(format,"11:59:00.000");

        if ((data.after(fascia5)  && data.before(fascia11))
                || (data.equals(fascia5) || data.equals(fascia11))){
            this.fascia="fascia5-11";

        }
        Date fascia12=utils.parseDate(format,"12:00:00.000");
        Date fascia19=utils.parseDate(format,"19:00:00.000");
        if ((data.after(fascia12)  && data.before(fascia19))
                || (data.equals(fascia12) || data.equals(fascia19))){
            this.fascia="fascia12-19";
        }





    }

}
