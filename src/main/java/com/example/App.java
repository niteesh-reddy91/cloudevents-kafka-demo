package com.example;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.jackson.JsonFormat;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Properties;
import java.util.UUID;

public class App {
    public static void main(String[] args) throws Exception {
        String topic = "demo-topic";

        PersonWorker person = new PersonWorker("John", "Doe");

        EventSpec<PersonWorker> eventSpec = new EventSpec<>(
            UUID.randomUUID().toString(),
            "com.example.udp.personworker.upsert",
            URI.create("/example/udp/personworker"),
            OffsetDateTime.now(),
            "PersonWorkerUpsert",
            "application/avro",
            "00095",
            "personworker",
            "abc123",
            "Person_Update",
            "PersonWorker",
            "2024-06-10T11:59:59Z",
            "abc1232024-06-10T11:59:59Z",
            "2.11",
            person
        );

        // Validate EventSpec and PersonWorker data before sending
        if (!eventSpec.isValid()) {
            System.err.println("EventSpec validation failed: missing required fields");
            return; // or throw exception
        }

        if (person.getFirstName() == null || person.getFirstName().trim().isEmpty() ||
            person.getLastName() == null || person.getLastName().trim().isEmpty()) {
            System.err.println("PersonWorker validation failed: first name or last name is missing or blank");
            return; // or throw exception
        }

        // Convert to CloudEvent and send if validations pass
        CloudEventEnvelope envelope = new CloudEventEnvelope();
        envelope.setId(eventSpec.getId());
        envelope.setType(eventSpec.getType());
        envelope.setSource(eventSpec.getSource().toString());
        envelope.setSubject(eventSpec.getSubject());
        envelope.setTime(eventSpec.getTime().toString());
        envelope.setClientid(eventSpec.getClientId());
        envelope.setTopic(eventSpec.getTopic());
        envelope.setData(eventSpec.getData());

        // Producer properties for Avro + Schema Registry
        Properties props = new Properties();
        props.put("bootstrap.servers", System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081"));

        // Add these timeout and retry configurations
        props.put("max.block.ms", 120000); // Wait up to 2 minutes for metadata
        props.put("request.timeout.ms", 30000); // 30 second request timeout
        props.put("retries", 5);
        props.put("retry.backoff.ms", 1000);
        props.put("delivery.timeout.ms", 120000);

        // Add client ID for better debugging
        props.put("client.id", "cloudevent-producer");

        // Enable idempotence for better reliability
        props.put("enable.idempotence", true);

        System.out.println("Connecting to Kafka at: " + props.get("bootstrap.servers"));
        System.out.println("Schema Registry at: " + props.get("schema.registry.url"));

        try (KafkaProducer<String, CloudEventEnvelope> producer = new KafkaProducer<>(props)) {
            // Test connectivity by listing topics (optional debug step)
            System.out.println("Producer created successfully, sending message...");
            
            // Send the person object directly - this is the Avro serialized payload
            ProducerRecord<String, CloudEventEnvelope> record = new ProducerRecord<>(topic, envelope);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Failed to send message: " + exception.getMessage());
                    exception.printStackTrace();
                } else {
                    System.out.println("Sent CloudEventEnvelope to Kafka topic " + topic);
                    System.out.println("Partition: " + metadata.partition() + ", Offset: " + metadata.offset());
                }
            });

            // Ensure message is sent before closing
            producer.flush();
            System.out.println("Message sent and flushed successfully");
        } catch (Exception e) {
            System.err.println("Error creating or using producer: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

