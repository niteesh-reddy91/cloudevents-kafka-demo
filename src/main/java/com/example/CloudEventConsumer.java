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

import com.example.CloudEvent;
import com.example.PersonWorkerData;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Map;
import com.example.SourcePlatform;

public class CloudEventConsumer {

    private static Schema schema;
    private static SchemaRegistryClient schemaRegistryClient;

    public static void main(String[] args) {
        String topic = "person-worker-events";
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

        System.out.println("Starting CloudEvent consumer with validation...");
        System.out.println("Bootstrap servers: " + props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        System.out.println("Schema Registry: " + props.get("schema.registry.url"));
        System.out.println("CloudEvent schema loaded from registry with validation rules");

        try (KafkaConsumer<String, CloudEvent> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            System.out.println("Subscribed to topic: " + topic);
            System.out.println("Waiting for CloudEvents on topic '" + topic + "'...");

            while (true) {
                ConsumerRecords<String, CloudEvent> records = consumer.poll(Duration.ofMillis(1000));
                
                if (records.isEmpty()) {
                    System.out.print(".");
                    continue;
                }

                System.out.println("\nReceived " + records.count() + " CloudEvent(s)");

                for (ConsumerRecord<String, CloudEvent> record : records) {
                    try {
                        System.out.println("\n" + "=".repeat(50));
                        System.out.println("Processing CloudEvent from partition " + record.partition() + ", offset " + record.offset());
                        
                        CloudEvent cloudEvent = record.value();
                        
                        // VALIDATE RECEIVED DATA USING SCHEMA REGISTRY SCHEMA
                        System.out.println("Validating received CloudEvent against Schema Registry rules...");
                        try {
                            GenericRecord cloudEventRecord = convertToGenericRecord(cloudEvent, schema);
                            AvroDataValidator.validate(cloudEventRecord, schema);
                            System.out.println("Received CloudEvent validation passed");
                        } catch (AvroDataValidator.ValidationException e) {
                            System.err.println("Received CloudEvent validation failed:");
                            for (String error : e.getErrors()) {
                                System.err.println("  - " + error);
                            }
                            // In production, you might want to send to dead letter queue
                            System.err.println("Skipping processing due to validation errors...");
                            continue; // Skip this record
                        }
                        
                        // DESERIALIZE AND PRINT CLOUDEVENT DATA
                        System.out.println("CloudEvent Details:");
                        System.out.println("  Spec Version: " + cloudEvent.getSpecversion());
                        System.out.println("  ID: " + cloudEvent.getId());
                        System.out.println("  Type: " + cloudEvent.getType());
                        System.out.println("  Source: " + cloudEvent.getSource());
                        System.out.println("  Data Content Type: " + cloudEvent.getDatacontenttype());
                        System.out.println("  Time: " + cloudEvent.getTime());
                        System.out.println("  Source Platform: " + cloudEvent.getSourceplatform());
                        System.out.println("  Source Platform ID: " + cloudEvent.getSourceplatformid());
                        System.out.println("  Client ID: " + cloudEvent.getClientid());

                        // ROUTE BASED ON TYPE AND SOURCE PLATFORM
                        String eventType = cloudEvent.getType();
                        String sourcePlatform = cloudEvent.getSourceplatform().toString();
                        String clientId = cloudEvent.getClientid();

                        System.out.println("Routing based on type: " + eventType + " and source platform: " + sourcePlatform);

                        if (isPersonWorkerEvent(eventType)) {
                            processPersonWorkerEvent(cloudEvent, clientId);
                        } else if (isOrderEvent(eventType)) {
                            processOrderEvent(cloudEvent, clientId);
                        } else if (isInventoryEvent(eventType)) {
                            processInventoryEvent(cloudEvent, clientId);
                        } else {
                            System.out.printf("Unhandled event - Type: %s, Source Platform: %s%n", eventType, sourcePlatform);
                            logUnhandledEvent(cloudEvent);
                        }

                        System.out.println("CloudEvent processed successfully");

                    } catch (Exception e) {
                        System.err.println("Error processing CloudEvent: " + e.getMessage());
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
     * Load CloudEvent schema from Schema Registry instead of local file
     */
    private static Schema loadSchemaFromRegistry(String topic, String schemaRegistryUrl) {
        try {
            String subject = topic + "-value";
            System.out.println("Fetching latest CloudEvent schema from Schema Registry for subject: " + subject);
            
            var latestSchema = schemaRegistryClient.getLatestSchemaMetadata(subject);
            System.out.println("Found schema version: " + latestSchema.getVersion() + ", ID: " + latestSchema.getId());
            
            Schema registrySchema = new Schema.Parser().parse(latestSchema.getSchema());
            
            // Verify this is a CloudEvent schema
            if (!"CloudEvent".equals(registrySchema.getName())) {
                throw new RuntimeException("Expected CloudEvent schema but got: " + registrySchema.getName());
            }
            
            // Verify validation rules are present
            ValidationRuleParser ruleParser = new ValidationRuleParser();
            Map<String, Object> validationRules = ruleParser.extractValidationRules(registrySchema);
            
            if (!validationRules.isEmpty()) {
                System.out.println("Validation rules found in CloudEvent schema");
                System.out.println("Available validation rules for fields: " + validationRules.keySet());
            } else {
                System.out.println("Warning: No validation rules found in CloudEvent schema");
            }
            
            return registrySchema;
            
        } catch (Exception e) {
            System.err.println("Failed to load CloudEvent schema from Schema Registry: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Cannot continue without CloudEvent schema", e);
        }
    }

    /**
     * Check if this is a PersonWorker related event
     */
    private static boolean isPersonWorkerEvent(String eventType) {
        return eventType.contains("person") || eventType.contains("PersonWorker");
    }

    /**
     * Check if this is an Order related event
     */
    private static boolean isOrderEvent(String eventType) {
        return eventType.contains("order") || eventType.contains("Order");
    }

    /**
     * Check if this is an Inventory related event
     */
    private static boolean isInventoryEvent(String eventType) {
        return eventType.contains("inventory") || eventType.contains("Inventory");
    }

    /**
     * Process PersonWorker events
     */
    private static void processPersonWorkerEvent(CloudEvent cloudEvent, String clientId) {
        System.out.println("Processing PersonWorker CloudEvent...");
        
        Object data = cloudEvent.getData();
        System.out.println("  Data type: " + (data != null ? data.getClass().getSimpleName() : "null"));
        
        if (data instanceof PersonWorkerData) {
            PersonWorkerData personData = (PersonWorkerData) data;
            
            System.out.println("  PersonWorker Data Details:");
            System.out.println("    GUPI: " + personData.getGupi());
            System.out.println("    First Name: " + personData.getFirstName());
            System.out.println("    Middle Name: " + personData.getMiddleName());
            System.out.println("    Last Name: " + personData.getLastName());
            System.out.println("    Prefix: " + personData.getPrefix());
            System.out.println("    Suffix: " + personData.getSuffix());
            System.out.println("    Birth Date: " + personData.getBirthDate());
            System.out.println("    Deceased Date: " + personData.getDeceasedDate());
            System.out.println("    Sex: " + personData.getSex());
            System.out.println("    Legal Name: " + personData.getLegalName());
            System.out.println("    Marital Status: " + personData.getMaritalStatus());

            // Additional business logic validation
            if (personData.getFirstName() == null || personData.getFirstName().trim().isEmpty() ||
                personData.getLastName() == null || personData.getLastName().trim().isEmpty()) {
                System.err.println("Business validation failed: PersonWorker data is incomplete");
                return;
            }

            // Route based on event type
            String eventType = cloudEvent.getType();
            if (eventType.contains("created")) {
                handlePersonWorkerCreated(personData, cloudEvent, clientId);
            } else if (eventType.contains("updated")) {
                handlePersonWorkerUpdated(personData, cloudEvent, clientId);
            } else if (eventType.contains("deleted")) {
                handlePersonWorkerDeleted(personData, cloudEvent, clientId);
            } else {
                System.out.println("Unknown PersonWorker event type: " + eventType);
            }
            
        } else {
            System.err.println("Expected PersonWorkerData but got: " + 
                (data != null ? data.getClass().getName() : "null"));
        }
    }

    /**
     * Handle PersonWorker Created events
     */
    private static void handlePersonWorkerCreated(PersonWorkerData personData, CloudEvent cloudEvent, String clientId) {
        System.out.println("Handling PersonWorker CREATED event");
        System.out.printf("Client %s: Creating new person %s %s (GUPI: %s)%n", 
            clientId, personData.getFirstName(), personData.getLastName(), personData.getGupi());
        
        // Business logic for creation
        System.out.println("Validating business rules for new person...");
        
        // Extract platform-specific information
        String sourcePlatform = cloudEvent.getSourceplatform().toString();
        String sourcePlatformId = cloudEvent.getSourceplatformid();
        System.out.printf("Source: %s (Platform ID: %s)%n", sourcePlatform, sourcePlatformId);
        
        System.out.println("Saving new person to database...");
        System.out.println("Sending welcome email...");
        System.out.println("Triggering onboarding workflow...");
        System.out.println("PersonWorker creation processed successfully");
    }

    /**
     * Handle PersonWorker Updated events
     */
    private static void handlePersonWorkerUpdated(PersonWorkerData personData, CloudEvent cloudEvent, String clientId) {
        System.out.println("Handling PersonWorker UPDATED event");
        System.out.printf("Client %s: Updating person %s %s (GUPI: %s)%n", 
            clientId, personData.getFirstName(), personData.getLastName(), personData.getGupi());
        
        // Extract platform-specific information
        String sourcePlatform = cloudEvent.getSourceplatform().toString();
        String sourcePlatformId = cloudEvent.getSourceplatformid();
        System.out.printf("Source: %s (Platform ID: %s)%n", sourcePlatform, sourcePlatformId);
        
        System.out.println("Updating person record in database...");
        System.out.println("Triggering change notifications...");
        System.out.println("Updating related systems...");
        System.out.println("PersonWorker update processed successfully");
    }

    /**
     * Handle PersonWorker Deleted events
     */
    private static void handlePersonWorkerDeleted(PersonWorkerData personData, CloudEvent cloudEvent, String clientId) {
        System.out.println("Handling PersonWorker DELETED event");
        System.out.printf("Client %s: Deleting person %s %s (GUPI: %s)%n", 
            clientId, personData.getFirstName(), personData.getLastName(), personData.getGupi());
        
        // Extract platform-specific information
        String sourcePlatform = cloudEvent.getSourceplatform().toString();
        String sourcePlatformId = cloudEvent.getSourceplatformid();
        System.out.printf("Source: %s (Platform ID: %s)%n", sourcePlatform, sourcePlatformId);
        
        System.out.println("Soft deleting person record...");
        System.out.println("Archiving related data...");
        System.out.println("Triggering cleanup processes...");
        System.out.println("PersonWorker deletion processed successfully");
    }

    /**
     * Process Order events (placeholder for future implementation)
     */
    private static void processOrderEvent(CloudEvent cloudEvent, String clientId) {
        System.out.println("Processing Order CloudEvent...");
        System.out.printf("Client %s: Order event received but not yet implemented%n", clientId);
        System.out.println("Event Type: " + cloudEvent.getType());
        System.out.println("Source Platform: " + cloudEvent.getSourceplatform());
        // TODO: Implement order event processing
    }

    /**
     * Process Inventory events (placeholder for future implementation)
     */
    private static void processInventoryEvent(CloudEvent cloudEvent, String clientId) {
        System.out.println("Processing Inventory CloudEvent...");
        System.out.printf("Client %s: Inventory event received but not yet implemented%n", clientId);
        System.out.println("Event Type: " + cloudEvent.getType());
        System.out.println("Source Platform: " + cloudEvent.getSourceplatform());
        // TODO: Implement inventory event processing
    }

    /**
     * Log unhandled events for monitoring and debugging
     */
    private static void logUnhandledEvent(CloudEvent cloudEvent) {
        System.out.println("Logging unhandled CloudEvent details:");
        System.out.println("  Type: " + cloudEvent.getType());
        System.out.println("  Source: " + cloudEvent.getSource());
        System.out.println("  Source Platform: " + cloudEvent.getSourceplatform());
        System.out.println("  Source Platform ID: " + cloudEvent.getSourceplatformid());
        System.out.println("  Client ID: " + cloudEvent.getClientid());
        System.out.println("  Spec Version: " + cloudEvent.getSpecversion());
        // In production, you might send this to a monitoring system or dead letter queue
    }

    /**
     * Convert CloudEvent to GenericRecord for validation
     */
    private static GenericRecord convertToGenericRecord(CloudEvent cloudEvent, Schema schema) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        
        // Set CloudEvent fields
        builder.set("specversion", cloudEvent.getSpecversion());
        builder.set("id", cloudEvent.getId());
        builder.set("source", cloudEvent.getSource());
        builder.set("type", cloudEvent.getType());
        builder.set("datacontenttype", cloudEvent.getDatacontenttype());
        builder.set("time", cloudEvent.getTime());
        builder.set("sourceplatform", cloudEvent.getSourceplatform());
        builder.set("sourceplatformid", cloudEvent.getSourceplatformid());
        builder.set("clientid", cloudEvent.getClientid());
        
        // Convert data field
        if (cloudEvent.getData() != null) {
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
                GenericRecord dataRecord = convertDataToGenericRecord(cloudEvent.getData(), recordSchema);
                builder.set("data", dataRecord);
            }
        }
        
        return builder.build();
    }

    /**
     * Convert data object to GenericRecord for validation
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
                // Fallback to manual mapping for known types
                if (data instanceof PersonWorkerData) {
                    PersonWorkerData personData = (PersonWorkerData) data;
                    
                    // Map all fields that exist in the schema
                    if (schema.getField("gupi") != null) {
                        builder.set("gupi", personData.getGupi());
                    }
                    if (schema.getField("firstName") != null) {
                        builder.set("firstName", personData.getFirstName());
                    }
                    if (schema.getField("middleName") != null) {
                        builder.set("middleName", personData.getMiddleName());
                    }
                    if (schema.getField("lastName") != null) {
                        builder.set("lastName", personData.getLastName());
                    }
                    if (schema.getField("suffix") != null) {
                        builder.set("suffix", personData.getSuffix());
                    }
                    if (schema.getField("prefix") != null) {
                        builder.set("prefix", personData.getPrefix());
                    }
                    if (schema.getField("birthDate") != null) {
                        builder.set("birthDate", personData.getBirthDate());
                    }
                    if (schema.getField("deceasedDate") != null) {
                        builder.set("deceasedDate", personData.getDeceasedDate());
                    }
                    if (schema.getField("sex") != null) {
                        builder.set("sex", personData.getSex());
                    }
                    if (schema.getField("legalName") != null) {
                        builder.set("legalName", personData.getLegalName());
                    }
                    if (schema.getField("maritalStatus") != null) {
                        builder.set("maritalStatus", personData.getMaritalStatus());
                    }
                } else {
                    throw new RuntimeException("Unsupported data type for conversion: " + data.getClass().getName());
                }
            }
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert data to GenericRecord. Data type: " + 
                data.getClass().getName() + ", Schema: " + schema.getName(), e);
        }
        
        return builder.build();
    }
}