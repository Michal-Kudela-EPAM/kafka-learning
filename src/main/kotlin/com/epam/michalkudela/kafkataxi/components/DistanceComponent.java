package com.epam.michalkudela.kafkataxi.components;

import com.epam.michalkudela.kafkataxi.model.VehicleDistance;
import com.epam.michalkudela.kafkataxi.model.VehiclePosition;
import com.google.gson.Gson;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@AllArgsConstructor
public class DistanceComponent {

    private final DistanceCache distanceCache;

    private final String distanceTopic;

    private final ReplyingKafkaTemplate<String, String, String> distanceKafkaReplying;

    private final Gson gson = new Gson();
    @KafkaListener(topics = "${kafka.position.topic}", groupId = "${kafka.group.id}", containerFactory = "kafkaJsonListenerContainerFactory")
    public String distanceListener(ConsumerRecord<String, String> consumerRecord) {
        VehiclePosition vehiclePosition = gson.fromJson(consumerRecord.value(), VehiclePosition.class);
        log.info("Got position: {}", vehiclePosition);
        VehicleDistance distance = calculateNewDistance(vehiclePosition);
        saveDistance(vehiclePosition, distance);
        return "ok";
    }

    private VehicleDistance calculateNewDistance(VehiclePosition vehiclePosition) {
        VehicleDistance lastDistance = distanceCache.getOrDefault(vehiclePosition.getId(), null);
        VehicleDistance.VehicleDistanceBuilder distance = VehicleDistance.builder()
                .id(vehiclePosition.getId())
                .yCoordinate(vehiclePosition.getyCoordinate())
                .xCoordinate(vehiclePosition.getxCoordinate());
        if(lastDistance == null){
            log.info("LastDistance is null");
            distance.distance(0.0);
        } else {
            log.info("LastDistance is {}", lastDistance);
            double distanceChange = calculateDistance(lastDistance, vehiclePosition);
            double distanceTotal = lastDistance.getDistance() + distanceChange;
            log.info("Distance change: {}, new total: {}", distanceChange, distanceTotal);
            distance.distance(distanceTotal);
        }

        return distance.build();
    }

    private double calculateDistance(VehicleDistance vehicleDistance, VehiclePosition vehiclePosition) {
        return Math.sqrt(
                Math.pow(vehicleDistance.getXCoordinate() - vehiclePosition.getxCoordinate(), 2.0) +
                Math.pow(vehicleDistance.getYCoordinate() - vehiclePosition.getyCoordinate(), 2.0)
        );
    }

    private void saveDistance(VehiclePosition vehiclePosition, VehicleDistance distance) {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                distanceTopic,
                vehiclePosition.getId(),
                gson.toJson(distance)
        );

        distanceKafkaReplying.send(record);
    }
}
