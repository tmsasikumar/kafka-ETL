package serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spike.kafkasteam.models.AggregatedEvent;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.Objects;

public class AggregatedEventSerializer implements Serializer<AggregatedEvent> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, AggregatedEvent data) {

        if (Objects.isNull(data)) {
            return null;
        }
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            System.out.println("Exception occured while serializing Event -> " + e.getMessage());
            return null;
        }
    }

    @Override
    public void close() {

    }
}


