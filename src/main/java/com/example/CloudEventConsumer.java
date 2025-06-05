package com.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import com.example.CloudEventEnvelope;
import com.example.PersonWorker;

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
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put("specific.avro.reader", true);
        props.put("schema.registry.url", "http://localhost:8081");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // Add timeout configurations
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);

        System.out.println("Starting consumer...");
        System.out.println("Bootstrap servers: " + props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        System.out.println("Schema Registry: " + props.get("schema.registry.url"));

        try (KafkaConsumer<String, CloudEventEnvelope> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            System.out.println("Subscribed to topic: " + topic);
            System.out.println("Waiting for CloudEvents on topic '" + topic + "'...");

            while (true) {
                ConsumerRecords<String, CloudEventEnvelope> records = consumer.poll(Duration.ofMillis(1000));
                
                if (records.isEmpty()) {
                    System.out.print(".");  // Show we're polling
                    continue;
                }

                System.out.println("\nReceived " + records.count() + " record(s)");

                for (ConsumerRecord<String, CloudEventEnvelope> record : records) {
                    try {
                        System.out.println("Processing record from partition " + record.partition() + ", offset " + record.offset());
                        
                        CloudEventEnvelope envelope = record.value();
                        System.out.println("Received CloudEventEnvelope:");
                        System.out.println("  ID: " + envelope.getId());
                        System.out.println("  Type: " + envelope.getType());
                        System.out.println("  Source: " + envelope.getSource());
                        System.out.println("  Topic: " + envelope.getTopic());
                        System.out.println("  Client ID: " + envelope.getClientid());
                        System.out.println("  Time: " + envelope.getTime());

                        // Routing logic based on metadata - Fixed the condition
                        String topicName = envelope.getTopic();
                        String clientId = envelope.getClientid();

                        // Check for both possible topic values based on your producer code
                        if ("Person_Update".equals(topicName) || "personworker".equals(topicName)) {
                            Object data = envelope.getData();
                            System.out.println("  Data type: " + (data != null ? data.getClass().getSimpleName() : "null"));
                            
                            if (data instanceof PersonWorker) {
                                PersonWorker person = (PersonWorker) data;
                                System.out.println("  Person data:");
                                System.out.println("    First Name: " + person.getFirstName());
                                System.out.println("    Last Name: " + person.getLastName());

                                if (person.getFirstName() == null || person.getFirstName().trim().isEmpty() ||
                                    person.getLastName() == null || person.getLastName().trim().isEmpty()) {
                                    System.err.println("Invalid PersonWorker data: first name or last name missing.");
                                    continue;
                                }

                                System.out.printf("Client %s: Processing person %s %s%n", clientId,
                                        person.getFirstName(), person.getLastName());
                            } else {
                                System.err.println("Data is not of type PersonWorker, got: " + 
                                    (data != null ? data.getClass().getName() : "null"));
                            }
                        } else {
                            System.out.printf("Unhandled topic: %s%n", topicName);
                        }

                    } catch (Exception e) {
                        System.err.println("Error processing CloudEventEnvelope: " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Consumer error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}