package com.spike.kafkasteam;

import com.spike.kafkasteam.deserializers.AggregatedEventDeserializer;
import com.spike.kafkasteam.deserializers.EventDeserializer;
import com.spike.kafkasteam.models.AggregatedEvent;
import com.spike.kafkasteam.models.Event;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import serializers.AggregatedEventSerializer;
import serializers.EventSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class KafkaETL {

    private static Serde<Event> eventSerde = Serdes.serdeFrom(new EventSerializer(), new EventDeserializer());
    private static Serde<AggregatedEvent> aggregatedEventSerde = Serdes.serdeFrom(new AggregatedEventSerializer(), new AggregatedEventDeserializer());
    private static Serde<String> stringSerde = Serdes.serdeFrom(String.class);

    static Properties getStreamsConfig() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-events");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }


    public static void main(final String[] args) {

        final StreamsBuilder builder = new StreamsBuilder();

        filterLandingEvent(builder);

        aggregateAndPublish(builder);

        final KafkaStreams streams = new KafkaStreams(builder.build(), getStreamsConfig());
        final CountDownLatch latch = new CountDownLatch(1);

        System.out.println("Starrted Processing");
        Runtime.getRuntime().addShutdownHook(new Thread("streams-events-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
        System.exit(0);
    }

    private static void filterLandingEvent(StreamsBuilder builder) {
        KStream<String, Event> transactions = builder.stream("events",
                Consumed.with(stringSerde, eventSerde));

        KStream<String, Event> video_landed = transactions.filter((k, v) -> v.getAction() != null && !v.getAction().equals("VIDEO_LANDED"));

        video_landed.to("filteredEvents", Produced.with(Serdes.String(), eventSerde));
    }

    private static void aggregateAndPublish(StreamsBuilder builder) {


        KTable<String, Event>  aggregated= builder.stream("filteredEvents", Consumed.with(stringSerde, eventSerde))
                .groupByKey(Grouped.with(stringSerde, eventSerde))
                .<Event>aggregate(new Initializer<Event>() {
                    @Override
                    public Event apply() {
                        return new Event();
                    }
                }, new Aggregator<String, Event, Event>() {
                    @Override
                    public Event apply(final String key, final Event event,final Event aggregate) {
                        long sum1 = event.getDuration() + aggregate.getDuration();
                        return new Event(event.getParticipationId(), event.getProductId(), sum1, event.getAction());
                    }
                }, Materialized.<String, Event, KeyValueStore<Bytes, byte[]>>as("counts-store").withKeySerde(stringSerde).withValueSerde(eventSerde));


        aggregated.toStream().to("aggregatedEvents", Produced.with(Serdes.String(), eventSerde));
    }
}



