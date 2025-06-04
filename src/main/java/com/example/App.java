package com.example;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.jackson.JsonFormat;

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
            "com.alight.udp.personworker.upsert",
            URI.create("/alight/udp/personworker"),
            OffsetDateTime.now(),
            "PersonWorkerUpsert",
            "application/json; charset=utf-8",
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

        if (person.getFirstName() == null || person.getFirstName().isBlank() ||
            person.getLastName() == null || person.getLastName().isBlank()) {
            System.err.println("PersonWorker validation failed: first name or last name is missing or blank");
            return; // or throw exception
        }

        // Convert to CloudEvent and send if validations pass
        CloudEvent cloudEvent = eventSpec.toCloudEvent();

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(JsonFormat.getCloudEventJacksonModule());
        String json = mapper.writeValueAsString(cloudEvent);

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (var producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props)) {
            var record = new org.apache.kafka.clients.producer.ProducerRecord<String, String>(topic, json);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                } else {
                    System.out.println("Sent CloudEvent to Kafka topic " + topic);
                    System.out.println("Payload: " + json);
                }
            });
        }
    }
}

