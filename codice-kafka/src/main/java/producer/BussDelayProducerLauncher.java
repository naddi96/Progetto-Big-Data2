package producer;

import java.io.FileInputStream;
import java.util.Scanner;

public class BussDelayProducerLauncher {
  //  $KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic stream


    private final static String TOPIC = "streams";
    //private final static String TOPIC = "t-multi-part";

    private final static int NUM_MESSAGES = 1000;
    private final static int SLEEP = 1000;


    public static void main(String[] args) {

        BussDelayKafkaProducer producer = new BussDelayKafkaProducer(TOPIC);

        try {

            //the file to be opened for reading
            FileInputStream fis=new FileInputStream("../producer/bus-breakdown-and-delays.csv");
            Scanner sc=new Scanner(fis);    //file to be scanned

            sc.nextLine();
            System.out.println("inizio stream tra 10 sec");
            Thread.sleep(10000);
            while(sc.hasNextLine())
            {
                String payload =  sc.nextLine();
                producer.produce(null, payload);
            //    Thread.sleep(SLEEP);
                //returns the line that was skipped
            }

        } catch ( Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }

    }


}
