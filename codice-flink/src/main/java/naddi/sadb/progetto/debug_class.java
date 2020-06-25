package naddi.sadb.progetto;

import naddi.sadb.progetto.utils.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;




public class debug_class {
    public static String week(String date){
        Date x =utils.parseDate("yyyy-MM-dd'T'HH:mm:ss.SSS",date);
        Calendar cal = Calendar.getInstance();
        cal.setTime(x);
        String week=String.valueOf(cal.get(Calendar.WEEK_OF_YEAR));
        return date.substring(0,4)+"-"+week;
    }


    public static void main(String[] args) throws ParseException {
        System.out.println("aaaaaaa".split(",")[0]);
        System.out.println("3000-09-07T07:41:00.000".substring(0,10));//giorno
        System.out.println("3000-09-07T07:41:00.000".substring(0,7));//mese
        System.out.println(week("3000-09-07T07:41:00.000"));//settimana
        System.out.println("3000-09-07T07:41:00.000".substring(0,13));//ora


        String format = "HH:mm:ss.SSS";
        String ex = "5:00:00.000";
        Date data= utils.parseDate(format,ex);
        Date fascia5=utils.parseDate(format,"5:00:00.000");
        Date fascia11=utils.parseDate(format,"11:59:00.000");

        if ((data.after(fascia5)  && data.before(fascia11) || data.equals(fascia5) || data.equals(fascia11))){
            System.out.println("aaaaa");
        }
        if (3>2){
            System.out.println(3);
        }

        System.out.println("dddddd");



        System.out.println(ex.substring(11));
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
