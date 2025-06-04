package com.example;

import io.cloudevents.CloudEvent;
import io.cloudevents.jackson.JsonFormat;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class CloudEventConsumer {

    public static void main(String[] args) {
        String topic = "demo-topic";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "cloudevents-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));

            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(JsonFormat.getCloudEventJacksonModule());

            System.out.println("Waiting for CloudEvents on topic '" + topic + "'...");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // Deserialize raw JSON string into CloudEvent
                        CloudEvent event = mapper.readValue(record.value(), CloudEvent.class);

                        // Deserialize the data into PersonWorker directly
                        byte[] eventData = event.getData().toBytes();
                        PersonWorker person = mapper.readValue(eventData, PersonWorker.class);

                        if (person == null || person.getFirstName() == null || person.getFirstName().isBlank() ||
                            person.getLastName() == null || person.getLastName().isBlank()) {
                            System.err.println("Invalid PersonWorker data: first name or last name missing.");
                            continue;
                        }

                        // Extract custom extensions
                        String clientId = event.getExtension("alightclientid").toString();
                        String topicName = event.getExtension("alighttopic").toString();

                        // Routing or logic
                        if ("Person_Update".equals(topicName)) {
                            System.out.printf("Client %s: Updating person %s %s%n", clientId,
                                    person.getFirstName(), person.getLastName());
                        } else {
                            System.out.printf("Unhandled topic: %s%n", topicName);
                        }

                    } catch (Exception e) {
                        System.err.println("Error processing CloudEvent: " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
