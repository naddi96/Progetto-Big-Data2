package SerilizerDeserialazer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Serializer;
import stream.BussDelay;

import java.util.Map;

public class SerializerBuss implements Serializer<BussDelay> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, BussDelay bussDelay) {
       return SerializationUtils.serialize(bussDelay);
    }
    @Override
    public void close() {

    }
}
