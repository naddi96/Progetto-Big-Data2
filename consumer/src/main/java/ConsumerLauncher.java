
public class ConsumerLauncher {

    private final static String TOPIC = "streams-wordcount-output";
    //private final static String TOPIC = "t-multi-part";


    public static void main(String[] args) {


            Thread c = new Thread(new SimpleKafkaConsumer(1, TOPIC));
            c.start();


    }

}
