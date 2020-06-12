package naddi.sadb.progetto;

public class prova {
    public static void main(String[] args){

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
