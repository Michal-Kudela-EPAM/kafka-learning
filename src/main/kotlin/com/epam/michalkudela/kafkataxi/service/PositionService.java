package com.epam.michalkudela.kafkataxi.service;

import com.epam.michalkudela.kafkataxi.model.VehiclePosition;
import com.google.gson.Gson;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class PositionService {
    private final String positionTopic;
    private final ReplyingKafkaTemplate<String, String, String> positionKafkaReplying;

    public void acceptVehicleSignal(VehiclePosition vehiclePosition) {
        validateRequest(vehiclePosition);
        savePosition(vehiclePosition);
    }

    private void validateRequest(VehiclePosition vehiclePosition) {
        if (
                vehiclePosition.getId() == null ||
                vehiclePosition.getxCoordinate() == null ||
                vehiclePosition.getyCoordinate() == null
        ) {
            throw new IllegalArgumentException();
        }
    }

    private void savePosition(VehiclePosition vehiclePosition) {
        Gson gson = new Gson();
        ProducerRecord<String, String> record = new ProducerRecord<>(
                positionTopic,
                vehiclePosition.getId(),
                gson.toJson(vehiclePosition)
//                vehiclePosition
        );
        positionKafkaReplying.send(record);
    }
}
