package com.epam.michalkudela.kafkataxi.components;

import com.epam.michalkudela.kafkataxi.model.VehicleDistance;
import com.google.gson.Gson;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
@Slf4j
public class DistanceObserver {
    private final Gson gson = new Gson();

    @KafkaListener(
            topics = "${kafka.distance.topic}",
            groupId = "${kafka.group.id}",
            containerFactory = "kafkaJsonListenerContainerFactory"
    )
    public String printNewDistance(ConsumerRecord<String, String> consumerRecord) {
        VehicleDistance vehicleDistance = gson.fromJson(consumerRecord.value(), VehicleDistance.class);
        log.info(
                "Vehicle id '{}' total distance: {}",
                vehicleDistance.getId(),
                vehicleDistance.getDistance()
        );
        return "ok";
    }
}