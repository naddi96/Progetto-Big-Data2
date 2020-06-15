package naddi.sadb.progetto;

import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class prova {
    public static void main(String[] args) throws ParseException {


        if (3>2){
            System.out.println(3);
        }

        System.out.println("dddddd");

        String format = "yyyy-MM-dd'T'HH:mm:ss.SSS";
        String ex="2019-11-27T14:45:00.000";
        SimpleDateFormat df = new SimpleDateFormat(format);
        Date dat = df.parse(ex);
        Calendar c = Calendar.getInstance();
        c.setTime(dat);

        System.out.println(c.getTimeInMillis());
        String m="45 min";
        if ((!m.contains(":") && m.toLowerCase().contains("m") && !(m.contains("-") || m.contains("/")))) {
            String stri = "";
            if (m.contains("/")) stri = "/";
            if (m.contains("-")) stri = "-";
            String[] numebers = m.split(stri);
            if (numebers.length > 1) {
                String num1 = numebers[0].replaceAll("[^0-9]", "");
                String num2 = numebers[1].replaceAll("[^0-9]", "");
                int num11 = Integer.valueOf(num1);
                int num22 = Integer.valueOf(num2);

                double x = (num11 + num22) / 2.0;

            }
        }


    }

}
