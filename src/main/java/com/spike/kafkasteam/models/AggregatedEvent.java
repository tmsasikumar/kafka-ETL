package com.spike.kafkasteam.models;


import lombok.Getter;

@Getter
public class AggregatedEvent {


    String participationId;
    String productId;
    String totalDuration;
    String action;
}



