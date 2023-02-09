package com.epam.michalkudela.kafkataxi.components;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@AllArgsConstructor
public class DistanceComponent {
    @KafkaListener(topics = "${kafka.position.topic}", groupId = "${kafka.group.id}", containerFactory = "kafkaJsonListenerContainerFactory")
//    @SendTo
    public String distanceListener(ConsumerRecord<String, String> vehiclePosition) {
        return "ok";
    }
}
