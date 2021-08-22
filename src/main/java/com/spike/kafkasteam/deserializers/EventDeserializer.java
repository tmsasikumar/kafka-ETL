package com.spike.kafkasteam.deserializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spike.kafkasteam.models.Event;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;
import java.util.Objects;

public class EventDeserializer implements Deserializer<Event> {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Event deserialize(String topic, byte[] bytes) {
        if (Objects.isNull(bytes)) {
            return null;
        }

        Event data = new Event();
        try {
            data = objectMapper.treeToValue(objectMapper.readTree(bytes), Event.class);
        } catch (Exception e) {
            System.out.println("Exception occured while deserializing Event -> " + e.getMessage());
        }

        return data;
    }

    @Override
    public void close() {

    }
}
