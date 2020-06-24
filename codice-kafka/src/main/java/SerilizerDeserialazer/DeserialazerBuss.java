package SerilizerDeserialazer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Deserializer;
import stream.BussDelay;

import java.util.Map;

public class DeserialazerBuss implements Deserializer<BussDelay> {
    private ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public BussDelay deserialize(String s, byte[] bytes) {
        BussDelay data = SerializationUtils.deserialize(bytes);
        return data;

    }

    @Override
    public void close() {

    }
}
