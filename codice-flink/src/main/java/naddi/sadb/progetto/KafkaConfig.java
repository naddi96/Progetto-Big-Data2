package naddi.sadb.progetto;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaConfig {

    public String brokers="localhost:9092,localhost:9093,localhost:9094";
    public String zookeeper="localhost:2181";
    public String input_topic ="streams";
    public String output_topic="streams-wordcount-output";

    public  Properties getProperties(){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", this.brokers);
        properties.setProperty("zookeeper.connect", this.zookeeper);
        return  properties;
    }

    public FlinkKafkaProducer<String> getProducer(){
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(
                this.brokers,            // broker list
                this.output_topic,                  // target topic
                new SimpleStringSchema());   // serialization schema
        myProducer.setWriteTimestampToKafka(true);
        return myProducer;
    }



}
