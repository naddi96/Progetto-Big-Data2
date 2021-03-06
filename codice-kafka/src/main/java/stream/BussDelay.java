package stream;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Data type for words with count.
 */
public class BussDelay implements Serializable {
    public    String Occurred_On;
    public    String How_Long_Delayed;
    public    String Boro;
    public    Long startingTimeNew;
    public    Long startingTimeOld;
    public long count;


    public  BussDelay(String line) {
            String[] x = line.split(";(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            this.Occurred_On=x[1+7];
            this.How_Long_Delayed=ritornaMinuti(x[11+1]).replaceAll("[^0-9]", "");
            this.Boro=x[9+1];
            this.count=1;
            this.startingTimeNew=Long.valueOf(x[0]);
            this.startingTimeOld=Long.valueOf(x[0]);
            //String c = "{'Occurred_On':" + Occurred_On + ", 'How_Long_Delayed':" + How_Long_Delayed + ", 'Boro':" + Boro + " 'count':"+1+"}";
           // return Occurred_On+"#"+How_Long_Delayed+"#"+Boro+"#"+"1";
    }



    private static String ritornaMinuti(String m){
        try {


            if (m.matches("[0-9]+")) {
                return m;
            }
            if ((!m.contains(":") && m.toLowerCase().contains("m") && (m.contains("-") || m.contains("/")))) {

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
                    return String.valueOf(x);
                }
            }

            if ((!m.contains(":") && m.toLowerCase().contains("m") && !(m.contains("/") || m.contains("-")))) {
                return String.valueOf(m.replaceAll("[^0-9]", ""));
            }
            if ((!m.contains(":") && (!(m.toLowerCase().contains("m") && m.toLowerCase().contains("h"))) && (m.contains("-") || m.contains("/")))) {
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
                    return String.valueOf(x);
                }
            }
            if (m.toLowerCase().contains("h") && !(m.contains("-") && m.contains("/"))) {
                if (m.contains(":")) {
                    ;
                    String[] x = m.split(":");
                    Integer num1 = Integer.valueOf(x[0].replaceAll("[^0-9]", ""));
                    Integer num2 = Integer.valueOf(x[1].replaceAll("[^0-9]", ""));
                    return String.valueOf(num1 * 60 + num2);
                }
                int num11 = Integer.valueOf(m.replaceAll("[^0-9]", ""));

                return String.valueOf(num11 * 60);
            }
            if (m.contains("1/2")) {
                return "30";
            }
            if (m.contains("45min/1hr")) {
                return "105";
            }
            if (m.contains("35min/45mi")) {
                return "40";
            }

        }catch (Exception e){
        }
        return "";

    }

/*
    static public String bussDelay(String line) {
        String[] x = line.split(";(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        String Occurred_On=x[7];
        String How_Long_Delayed=ritornaMinuti(x[11]).replaceAll("[^0-9]", "");
        String Boro=x[9];
        String c = "{'Occurred_On':" + Occurred_On + ", 'How_Long_Delayed':" + How_Long_Delayed + ", 'Boro':" + Boro + " 'count':"+1+"}";
        return Occurred_On+"#"+How_Long_Delayed+"#"+Boro+"#"+"1";
    }

*/
}