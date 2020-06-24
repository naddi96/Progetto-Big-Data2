package SerilizerDeserialazer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import stream.BussDelay;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeExstractor implements TimestampExtractor {

    @Override
    public long extract(final ConsumerRecord<Object, Object> record, final long previousTimestamp) {
        // `Foo` is your own custom class, which we assume has a method that returns
        // the embedded timestamp (milliseconds since midnight, January 1, 1970 UTC).
        long timestamp = -1;


            String sa = (String) record.value();
            String[] x = sa.split(";(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

            //System.out.println(x[7]);
            Date k = parseDate("yyyy-MM-dd'T'HH:mm:ss.SSS", x[7+1]);
            return k.getTime();

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

}