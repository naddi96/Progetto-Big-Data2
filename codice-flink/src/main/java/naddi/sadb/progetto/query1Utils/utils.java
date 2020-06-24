package naddi.sadb.progetto.query1Utils;

import org.apache.flink.api.java.tuple.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class utils {


    public static String returnMinDate(String date1, String date2){

        String format = "yyyy-MM-dd'T'HH:mm:ss.SSS";

        SimpleDateFormat df = new SimpleDateFormat(format);
        Date dat1 = parseDate(format,date1);
        Date dat2 = parseDate(format,date2);
        if (dat1.getTime() > dat2.getTime()){
            return  date2;
        }
        return date1;
    }


    public static Date parseDate(String format,String date){
        SimpleDateFormat df = new SimpleDateFormat(format);
        Date dat = null;
        try {
            dat = df.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return dat;
    }

    public static Tuple2<String,Integer> exstractMax(List<Tuple2<String,Integer>> lis){
        Tuple2<String,Integer> max=new Tuple2<>("",0);
        for (Tuple2<String,Integer> x:lis){
            if (max.f1.intValue() <= x.f1.intValue()){
                max=x;
            }
        }
        return max;

    }


}
