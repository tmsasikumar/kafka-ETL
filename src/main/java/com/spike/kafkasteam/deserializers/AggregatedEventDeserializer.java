package com.spike.kafkasteam.deserializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spike.kafkasteam.models.AggregatedEvent;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;
import java.util.Objects;

public class AggregatedEventDeserializer implements Deserializer<AggregatedEvent> {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public AggregatedEvent deserialize(String topic, byte[] bytes) {
        if (Objects.isNull(bytes)) {
            return null;
        }

        AggregatedEvent data = new AggregatedEvent();
        try {
            data = objectMapper.treeToValue(objectMapper.readTree(bytes), AggregatedEvent.class);
        } catch (Exception e) {
            System.out.println("Exception occured while deserializing AggregatedEvent -> " + e.getMessage());
        }

        return data;
    }

    @Override
    public void close() {

    }
}
