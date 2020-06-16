package naddi.sadb.progetto;

import org.apache.flink.api.java.tuple.Tuple2;
import java.util.ArrayList;
import java.util.List;

public class BussDisservice {
    public String Occurred_On;
    public String Reason;
    public int rank=1;

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

    public String fascia;
    public List<Tuple2<String, Integer>> merge_fascia_list= new ArrayList<Tuple2<String, Integer>>();
    public BussDisservice() {}

    public BussDisservice(String occurred_On, String reason,String fascia) {
        Occurred_On = occurred_On;
        Reason = reason;
        this.fascia =fascia;
    }

}
