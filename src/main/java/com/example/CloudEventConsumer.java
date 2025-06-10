package com.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;

import com.example.CloudEventEnvelope;
import com.example.PersonWorker;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Map;

public class CloudEventConsumer {

    private static Schema schema;
    private static SchemaRegistryClient schemaRegistryClient;

    public static void main(String[] args) {
        String topic = "demo-topic";
        String schemaRegistryUrl = System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081");

        // Initialize Schema Registry client
        schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 1000);

        // Load schema from Schema Registry with validation rules
        schema = loadSchemaFromRegistry(topic, schemaRegistryUrl);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 
                 System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "cloudevents-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put("specific.avro.reader", true);
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);

        System.out.println("Starting consumer with validation...");
        System.out.println("Bootstrap servers: " + props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        System.out.println("Schema Registry: " + props.get("schema.registry.url"));
        System.out.println("Schema loaded from registry with validation rules");

        try (KafkaConsumer<String, CloudEventEnvelope> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            System.out.println("Subscribed to topic: " + topic);
            System.out.println("Waiting for CloudEvents on topic '" + topic + "'...");

            while (true) {
                ConsumerRecords<String, CloudEventEnvelope> records = consumer.poll(Duration.ofMillis(1000));
                
                if (records.isEmpty()) {
                    System.out.print(".");
                    continue;
                }

                System.out.println("\nReceived " + records.count() + " record(s)");

                for (ConsumerRecord<String, CloudEventEnvelope> record : records) {
                    try {
                        System.out.println("\n" + "=".repeat(50));
                        System.out.println("Processing record from partition " + record.partition() + ", offset " + record.offset());
                        
                        CloudEventEnvelope envelope = record.value();
                        
                        // VALIDATE RECEIVED DATA USING SCHEMA REGISTRY SCHEMA
                        System.out.println("Validating received data against Schema Registry rules...");
                        try {
                            GenericRecord envelopeRecord = convertToGenericRecord(envelope, schema);
                            AvroDataValidator.validate(envelopeRecord, schema);
                            System.out.println("Received data validation passed");
                        } catch (AvroDataValidator.ValidationException e) {
                            System.err.println("Received data validation failed:");
                            for (String error : e.getErrors()) {
                                System.err.println("  - " + error);
                            }
                            // In production, you might want to send to dead letter queue
                            System.err.println("Skipping processing due to validation errors...");
                            continue; // Skip this record
                        }
                        
                        // DESERIALIZE AND PRINT ENVELOPE DATA
                        System.out.println("CloudEventEnvelope Details:");
                        System.out.println("  ID: " + envelope.getId());
                        System.out.println("  Type: " + envelope.getType());
                        System.out.println("  Source: " + envelope.getSource());
                        System.out.println("  Subject: " + envelope.getSubject());
                        System.out.println("  Topic: " + envelope.getTopic());
                        System.out.println("  Client ID: " + envelope.getClientid());
                        System.out.println("  Time: " + envelope.getTime());

                        // ROUTE BASED ON TOPIC AND TYPE
                        String topicName = envelope.getTopic();
                        String eventType = envelope.getType();
                        String clientId = envelope.getClientid();

                        System.out.println("Routing based on topic: " + topicName + " and type: " + eventType);

                        if (isPersonWorkerEvent(topicName, eventType)) {
                            processPersonWorkerEvent(envelope, clientId);
                        } else if (isOrderEvent(topicName, eventType)) {
                            processOrderEvent(envelope, clientId);
                        } else if (isInventoryEvent(topicName, eventType)) {
                            processInventoryEvent(envelope, clientId);
                        } else {
                            System.out.printf("Unhandled event - Topic: %s, Type: %s%n", topicName, eventType);
                            logUnhandledEvent(envelope);
                        }

                        System.out.println("Record processed successfully");

                    } catch (Exception e) {
                        System.err.println("Error processing CloudEventEnvelope: " + e.getMessage());
                        e.printStackTrace();
                        // In production, you might want to send to dead letter queue
                    }
                }
                
                // Commit offsets after successful processing
                consumer.commitSync();
                System.out.println("Committed offsets for processed records");
            }
        } catch (Exception e) {
            System.err.println("Consumer error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Load schema from Schema Registry instead of local file
     */
    private static Schema loadSchemaFromRegistry(String topic, String schemaRegistryUrl) {
        try {
            String subject = topic + "-value";
            System.out.println("Fetching latest schema from Schema Registry for subject: " + subject);
            
            var latestSchema = schemaRegistryClient.getLatestSchemaMetadata(subject);
            System.out.println("Found schema version: " + latestSchema.getVersion() + ", ID: " + latestSchema.getId());
            
            Schema registrySchema = new Schema.Parser().parse(latestSchema.getSchema());
            
            // Verify validation rules are present
            ValidationRuleParser ruleParser = new ValidationRuleParser();
            Map<String, Object> validationRules = ruleParser.extractValidationRules(registrySchema);
            
            if (!validationRules.isEmpty()) {
                System.out.println("Validation rules found in Schema Registry schema");
                System.out.println("Available validation rules for fields: " + validationRules.keySet());
            } else {
                System.out.println("Warning: No validation rules found in Schema Registry schema");
            }
            
            return registrySchema;
            
        } catch (Exception e) {
            System.err.println("Failed to load schema from Schema Registry: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Cannot continue without schema", e);
        }
    }

    /**
     * Check if this is a PersonWorker related event
     */
    private static boolean isPersonWorkerEvent(String topic, String eventType) {
        return ("person-worker-events".equals(topic) || 
                "personworker".equals(topic) ||
                "Person_Update".equals(topic)) &&
               (eventType.contains("PersonWorker") || eventType.contains("Person"));
    }

    /**
     * Check if this is an Order related event
     */
    private static boolean isOrderEvent(String topic, String eventType) {
        return "order-events".equals(topic) && eventType.contains("Order");
    }

    /**
     * Check if this is an Inventory related event
     */
    private static boolean isInventoryEvent(String topic, String eventType) {
        return "inventory-events".equals(topic) && eventType.contains("Inventory");
    }

    /**
     * Process PersonWorker events
     */
    private static void processPersonWorkerEvent(CloudEventEnvelope envelope, String clientId) {
        System.out.println("Processing PersonWorker Event...");
        
        Object data = envelope.getData();
        System.out.println("  Data type: " + (data != null ? data.getClass().getSimpleName() : "null"));
        
        if (data instanceof PersonWorker) {
            PersonWorker person = (PersonWorker) data;
            
            System.out.println("  PersonWorker Details:");
            System.out.println("    UUID: " + person.getUuid());
            System.out.println("    First Name: " + person.getFirstName());
            System.out.println("    Last Name: " + person.getLastName());
            System.out.println("    Age: " + person.getAge());
            System.out.println("    Email: " + person.getEmail());
            System.out.println("    Status: " + person.getStatus());
            System.out.println("    Sex: " + person.getSex());
            System.out.println("    Birthdate: " + person.getBirthdate());

            // Additional business logic validation
            if (person.getFirstName() == null || person.getFirstName().trim().isEmpty() ||
                person.getLastName() == null || person.getLastName().trim().isEmpty()) {
                System.err.println("Business validation failed: PersonWorker data is incomplete");
                return;
            }

            // Route based on event type
            String eventType = envelope.getType();
            if (eventType.contains("Created")) {
                handlePersonWorkerCreated(person, envelope, clientId);
            } else if (eventType.contains("Updated")) {
                handlePersonWorkerUpdated(person, envelope, clientId);
            } else if (eventType.contains("Deleted")) {
                handlePersonWorkerDeleted(person, envelope, clientId);
            } else {
                System.out.println("Unknown PersonWorker event type: " + eventType);
            }
            
        } else {
            System.err.println("Expected PersonWorker data but got: " + 
                (data != null ? data.getClass().getName() : "null"));
        }
    }

    /**
     * Handle PersonWorker Created events
     */
    private static void handlePersonWorkerCreated(PersonWorker person, CloudEventEnvelope envelope, String clientId) {
        System.out.println("Handling PersonWorker CREATED event");
        System.out.printf("Client %s: Creating new person %s %s (UUID: %s)%n", 
            clientId, person.getFirstName(), person.getLastName(), person.getUuid());
        
        // Business logic for creation
        System.out.println("Validating business rules for new person...");
        if (person.getAge() != 0 && (person.getAge() < 18 || person.getAge() > 65)) {
            System.err.println("Business rule violation: Age must be between 18 and 65");
            return;
        }
        
        System.out.println("Saving new person to database...");
        System.out.println("Sending welcome email...");
        System.out.println("Triggering onboarding workflow...");
        System.out.println("PersonWorker creation processed successfully");
    }

    /**
     * Handle PersonWorker Updated events
     */
    private static void handlePersonWorkerUpdated(PersonWorker person, CloudEventEnvelope envelope, String clientId) {
        System.out.println("Handling PersonWorker UPDATED event");
        System.out.printf("Client %s: Updating person %s %s (UUID: %s)%n", 
            clientId, person.getFirstName(), person.getLastName(), person.getUuid());
        
        System.out.println("Updating person record in database...");
        System.out.println("Triggering change notifications...");
        System.out.println("Updating related systems...");
        System.out.println("PersonWorker update processed successfully");
    }

    /**
     * Handle PersonWorker Deleted events
     */
    private static void handlePersonWorkerDeleted(PersonWorker person, CloudEventEnvelope envelope, String clientId) {
        System.out.println("Handling PersonWorker DELETED event");
        System.out.printf("Client %s: Deleting person %s %s (UUID: %s)%n", 
            clientId, person.getFirstName(), person.getLastName(), person.getUuid());
        
        System.out.println("Soft deleting person record...");
        System.out.println("Archiving related data...");
        System.out.println("Triggering cleanup processes...");
        System.out.println("PersonWorker deletion processed successfully");
    }

    /**
     * Process Order events (placeholder for future implementation)
     */
    private static void processOrderEvent(CloudEventEnvelope envelope, String clientId) {
        System.out.println("Processing Order Event...");
        System.out.printf("Client %s: Order event received but not yet implemented%n", clientId);
        // TODO: Implement order event processing
    }

    /**
     * Process Inventory events (placeholder for future implementation)
     */
    private static void processInventoryEvent(CloudEventEnvelope envelope, String clientId) {
        System.out.println("Processing Inventory Event...");
        System.out.printf("Client %s: Inventory event received but not yet implemented%n", clientId);
        // TODO: Implement inventory event processing
    }

    /**
     * Log unhandled events for monitoring and debugging
     */
    private static void logUnhandledEvent(CloudEventEnvelope envelope) {
        System.out.println("Logging unhandled event details:");
        System.out.println("  Topic: " + envelope.getTopic());
        System.out.println("  Type: " + envelope.getType());
        System.out.println("  Source: " + envelope.getSource());
        System.out.println("  Subject: " + envelope.getSubject());
        System.out.println("  Client ID: " + envelope.getClientid());
        // In production, you might send this to a monitoring system or dead letter queue
    }

    /**
     * Convert CloudEventEnvelope to GenericRecord for validation using Avro built-in conversion
     * Updated to use the same approach as the producer
     */
    private static GenericRecord convertToGenericRecord(CloudEventEnvelope envelope, Schema schema) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        
        // Set envelope-level fields
        builder.set("id", envelope.getId());
        builder.set("source", envelope.getSource());
        builder.set("type", envelope.getType());
        builder.set("subject", envelope.getSubject());
        builder.set("time", envelope.getTime());
        builder.set("clientid", envelope.getClientid());
        builder.set("topic", envelope.getTopic());
        
        // Convert data field using the same logic as the producer
        if (envelope.getData() != null) {
            Schema dataFieldSchema = schema.getField("data").schema();
            
            // Handle union type for data field (null or specific record type)
            Schema recordSchema = null;
            if (dataFieldSchema.getType() == Schema.Type.UNION) {
                for (Schema unionType : dataFieldSchema.getTypes()) {
                    if (unionType.getType() == Schema.Type.RECORD) {
                        recordSchema = unionType;
                        break;
                    }
                }
            } else if (dataFieldSchema.getType() == Schema.Type.RECORD) {
                recordSchema = dataFieldSchema;
            }
            
            if (recordSchema != null) {
                GenericRecord dataRecord = convertDataToGenericRecord(envelope.getData(), recordSchema);
                builder.set("data", dataRecord);
            }
        }
        
        return builder.build();
    }

    /**
     * Generic method to convert any domain object to GenericRecord using Avro built-in conversion
     * This is the same method as in the producer for consistency
     */
    private static GenericRecord convertDataToGenericRecord(Object data, Schema schema) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        
        try {
            // Check if the data object is a SpecificRecord (Avro-generated class)
            if (data instanceof org.apache.avro.specific.SpecificRecord) {
                org.apache.avro.specific.SpecificRecord avroRecord = (org.apache.avro.specific.SpecificRecord) data;
                Schema dataSchema = avroRecord.getSchema();
                
                // Use Avro's built-in conversion - iterate through target schema fields
                for (Schema.Field targetField : schema.getFields()) {
                    String fieldName = targetField.name();
                    
                    // Check if the source Avro record has this field
                    Schema.Field sourceField = dataSchema.getField(fieldName);
                    if (sourceField != null) {
                        // Get the value using Avro's get method
                        Object fieldValue = avroRecord.get(sourceField.pos());
                        
                        if (fieldValue != null) {
                            // Handle enum conversion if needed
                            if (fieldValue instanceof org.apache.avro.generic.GenericEnumSymbol) {
                                // Convert Avro enum to string
                                builder.set(fieldName, fieldValue.toString());
                            } else if (fieldValue instanceof Enum) {
                                // Convert Java enum to string
                                builder.set(fieldName, fieldValue.toString());
                            } else {
                                // Set the value directly
                                builder.set(fieldName, fieldValue);
                            }
                        } else {
                            // Handle null values - check if field has default
                            if (targetField.hasDefaultValue()) {
                                builder.set(fieldName, targetField.defaultVal());
                            }
                            // If no default and null, GenericRecordBuilder will handle it
                        }
                    } else {
                        // Field doesn't exist in source, check if target has default
                        if (targetField.hasDefaultValue()) {
                            builder.set(fieldName, targetField.defaultVal());
                        }
                    }
                }
                
            } else {
                // Fallback for non-Avro objects (though this shouldn't happen in your use case)
                throw new IllegalArgumentException("Expected SpecificRecord but got: " + data.getClass().getName());
            }
            
        } catch (Exception e) {
            // Final fallback to manual mapping for known types
            if (data instanceof PersonWorker) {
                PersonWorker person = (PersonWorker) data;
                
                // Map all fields that exist in the schema
                if (schema.getField("uuid") != null) {
                    builder.set("uuid", person.getUuid());
                }
                if (schema.getField("firstName") != null) {
                    builder.set("firstName", person.getFirstName());
                }
                if (schema.getField("lastName") != null) {
                    builder.set("lastName", person.getLastName());
                }
                if (schema.getField("age") != null) {
                    builder.set("age", person.getAge());
                }
                if (schema.getField("email") != null) {
                    builder.set("email", person.getEmail());
                }
                if (schema.getField("status") != null) {
                    builder.set("status", person.getStatus());
                }
                if (schema.getField("sex") != null && person.getSex() != null) {
                    builder.set("sex", person.getSex().toString());
                }
                if (schema.getField("birthdate") != null) {
                    builder.set("birthdate", person.getBirthdate());
                }
            } else {
                throw new RuntimeException("Failed to convert data to GenericRecord. Data type: " + 
                    data.getClass().getName() + ", Schema: " + schema.getName(), e);
            }
        }
        
        return builder.build();
    }
}