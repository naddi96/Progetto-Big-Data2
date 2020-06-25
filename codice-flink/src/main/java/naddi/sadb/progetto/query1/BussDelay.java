package naddi.sadb.progetto.query1;


import java.io.Serializable;

/**
 * Data type for words with count.
 */
public class BussDelay implements Serializable {
    public String How_Long_Delayed;
    public String Boro;
    public String Occurred_On;
    public int count=1;
    public    Long startingTimeNew;
    public    Long startingTimeOld;

    public BussDelay() {}

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
    public BussDelay(String line) {
        String[] x = line.split(";(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        this.Occurred_On=x[7+1];
        this.How_Long_Delayed=ritornaMinuti(x[11+1]).replaceAll("[^0-9]", "");
        this.Boro=x[9+1];
        this.startingTimeNew=Long.valueOf(x[0]);
        this.startingTimeOld=Long.valueOf(x[0]);
    }

    @Override
    public String toString() {
        return this.How_Long_Delayed + " : " + this.Boro + " : " + this.Occurred_On +" : "+ this.count;
    }
}