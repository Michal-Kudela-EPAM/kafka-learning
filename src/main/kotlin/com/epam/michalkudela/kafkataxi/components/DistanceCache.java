package com.epam.michalkudela.kafkataxi.components;

import com.epam.michalkudela.kafkataxi.model.VehicleDistance;
import com.google.gson.Gson;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
@AllArgsConstructor
@Slf4j
public class DistanceCache {
    private final ConcurrentMap<String, VehicleDistance> cache = new ConcurrentHashMap<>();
    private final Gson gson = new Gson();

    @KafkaListener(
            groupId = "${kafka.group.id}",
            containerFactory = "kafkaJsonListenerContainerFactory",
            autoStartup = "true",
            topicPartitions = @TopicPartition(
                    topic = "${kafka.distance.topic}",
                    partitionOffsets = {
                            @PartitionOffset(partition = "0", initialOffset = "0"),
                            @PartitionOffset(partition = "1", initialOffset = "0")
                    }
            ),
            properties = {
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "=earliest",
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG + "=false"
            }
    )
    public void updateCache(ConsumerRecord<String, String> consumerRecord) {
        VehicleDistance vehicleDistance = gson.fromJson(consumerRecord.value(), VehicleDistance.class);
        cache.put(vehicleDistance.getId(), vehicleDistance);
        log.info("Updated cache for vehicle with id '{}'", vehicleDistance.getId());
    }

    public VehicleDistance getOrDefault(String vehicleId, VehicleDistance defaultValue) {
        return cache.getOrDefault(vehicleId, defaultValue);
    }
}
