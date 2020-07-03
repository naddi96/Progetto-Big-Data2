import config.Configuration;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class SimpleKafkaConsumer implements Runnable {

//    private final static String TOPIC = "a-simple-testing-topic";

    private final static String CONSUMER_GROUP_ID = "simple-consumer2";

    private Consumer<String, String> consumer;
    private int id;
    private String topic;
    //private  int mex_daprocessare=1060;
    //private  int mex_processati=0;
    private String tempi="";

    public SimpleKafkaConsumer(int id, String topic){

        this.id = id;
        this.topic = topic;
        consumer = createConsumer();

        subscribeToTopic();

    }

    private Consumer<String, String> createConsumer() {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                Configuration.BOOTSTRAP_SERVERS);

        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                CONSUMER_GROUP_ID);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        return new KafkaConsumer<>(props);
    }

    private void subscribeToTopic(){

        // To consume data, we first need to subscribe to the topics of interest
        consumer.subscribe(Collections.singletonList(this.topic));

    }

    public void listTopics() {

        Map<String, List<PartitionInfo>> topics = consumer.listTopics();
        for (String topicName : topics.keySet()) {

            if (topicName.startsWith("__"))
                continue;

            List<PartitionInfo> partitions = topics.get(topicName);
            for (PartitionInfo partition : partitions) {
                System.out.println("Topic: " +
                        topicName + "; Partition: " + partition.toString());
            }

        }

    }

    public static void appendStrToFile(String fileName,
                                       String str)
    {
        try {

            BufferedWriter out = new BufferedWriter(
                    new FileWriter(fileName, true));
            out.write(str+"\n");
            out.close();
        }
        catch (IOException e) {
            System.out.println("exception occoured" + e);
        }
    }
    public void run() {

        boolean running = true;
        System.out.println("Consumer " + id + " running...");
        try {
            while (running) {
                //Thread.sleep(1000);
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(0));
                for (ConsumerRecord<String, String> record : records){
                    long end = System.nanoTime();
                    String[] cx = record.value().split(",");
                    String newTime = cx[0];
                    String oldTime = cx[1];
                    float timenew = (float)(end - Long.valueOf(newTime))/1000000000 ;
                    float tmieold =(float) (end -Long.valueOf(oldTime))/1000000000 ;
                    System.out.println(timenew+","+tmieold);
                    //save to file
                    appendStrToFile("output.csv",record.value().substring(28,record.value().length()));
                    //System.out.println("tempo di latenza da recod più vecchio: "+tmieold  );
                    //System.out.println("tempo di latenza da recod più nuovo: "+timenew  );
                    //System.out.println(record.value().substring(28,record.value().length()));
                    //tempi=tempi+timenew+","+tmieold+"\n";
                    //mex_processati++;

                }
                /*if(mex_processati == mex_daprocessare){
                    System.out.println("finito");
                    appendStrToFile("output.csv",tempi);
                    break;
                }*/

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("aaaa");
            consumer.close();
        }

    }

}
