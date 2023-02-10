package com.epam.michalkudela.kafkataxi;

import com.epam.michalkudela.kafkataxi.model.VehiclePosition;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfiguration {
    @Value("${kafka.group.id}")
    private String groupId;

    @Value("${kafka.position.topic}")
    private String positionTopic;

    @Value("${kafka.distance.topic}")
    private String distanceTopic;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Primary
    @Bean
    public ReplyingKafkaTemplate<String, String, String> positionKafkaReplying(
            ProducerFactory<String, String> producerFactory,
            ConcurrentKafkaListenerContainerFactory<String, String> listenerFactory
    ) {
        return getReplyingKafkaTemplate(listenerFactory, positionTopic, producerFactory);
    }

    @Bean
    public ReplyingKafkaTemplate<String, VehiclePosition, String> distanceKafkaReplying(
            ProducerFactory<String, VehiclePosition> producerFactory,
            ConcurrentKafkaListenerContainerFactory<String, String> listenerFactory
    ) {
        return getReplyingKafkaTemplate(listenerFactory, distanceTopic, producerFactory);
    }

    private <V> ReplyingKafkaTemplate<String, V, String> getReplyingKafkaTemplate(
            ConcurrentKafkaListenerContainerFactory<String, String> listenerFactory,
            String distanceTopic,
            ProducerFactory<String, V> producerFactory
    ) {
        var replyContainer = listenerFactory.createContainer(distanceTopic);
        replyContainer.getContainerProperties().setMissingTopicsFatal(false);
        replyContainer.getContainerProperties().setGroupId(groupId);
        return new ReplyingKafkaTemplate<>(producerFactory, replyContainer);
    }

    @Bean
    public String positionTopic() {
        return positionTopic;
    }

    @Bean
    public String distanceTopic() {
        return distanceTopic;
    }

    @Bean
    @Primary
    public KafkaListenerContainerFactory<?> kafkaJsonListenerContainerFactory(
            KafkaProperties properties
    ) {
        ConcurrentKafkaListenerContainerFactory<String, VehiclePosition> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(
                consumerFactory(properties)
        );
        factory.setMessageConverter(new StringJsonMessageConverter());
        return factory;
    }

    @Bean
    public ConsumerFactory<String, VehiclePosition> consumerFactory(
            KafkaProperties properties
    ) {
        Map<String, Object> props = properties.buildConsumerProperties();
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public Map<String, Object> consumerConfigs() {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,

                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class
        );
    }
}
