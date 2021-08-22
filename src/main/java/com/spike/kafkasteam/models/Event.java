package com.spike.kafkasteam.models;


import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class Event {


    String participationId;
    String productId;
    Long duration;
    String action;

    public Event() {
        this.duration = Long.valueOf(0);
    }
}



