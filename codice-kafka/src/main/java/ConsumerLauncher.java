
public class ConsumerLauncher {

    private final static String TOPIC = "streams-wordcount-output";
    //private final static String TOPIC = "t-multi-part";
    private static final int NUM_CONSUMERS = 3;

    public static void main(String[] args) {


            Thread c = new Thread(new SimpleKafkaConsumer(1, TOPIC));
            c.start();


    }

}
