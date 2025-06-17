package com.example.listener;

import com.example.CloudEvent;
import com.example.PersonWorkerData;
import com.example.AvroDataValidator;
import com.example.ValidationRuleParser;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.github.springwolf.core.asyncapi.annotations.AsyncListener;
import io.github.springwolf.core.asyncapi.annotations.AsyncOperation;
import io.github.springwolf.bindings.kafka.annotations.KafkaAsyncOperationBinding;
import org.springframework.messaging.handler.annotation.Payload;

import java.util.Map;

@Component
public class CloudEventKafkaListener {

    private final SchemaRegistryClient schemaRegistryClient;
    private Schema schema;

    @Autowired
    public CloudEventKafkaListener(SchemaRegistryClient schemaRegistryClient) {
        this.schemaRegistryClient = schemaRegistryClient;
    }

    @AsyncListener(operation = @AsyncOperation(
        channelName = "person-worker-events",
        description = "Handles CloudEvent messages for PersonWorker domain operations (create, update, delete). Includes Kafka headers: partition, offset, topic, timestamp, groupId"
    ))
    @KafkaAsyncOperationBinding(
        bindingVersion = "${app.kafka.binding.version:0.5.0}",
        groupId = "${app.kafka.consumer.group-id:cloudevents-consumer-group}",
        clientId = "${app.kafka.consumer.client-id:${spring.application.name}-consumer}"
    )
    @KafkaListener(topics = "${app.kafka.topic.person-worker-events:person-worker-events}",
                   groupId = "${app.kafka.consumer.group-id:cloudevents-consumer-group}",
                   containerFactory = "kafkaListenerContainerFactory")
    public void handleCloudEvent(@Payload CloudEvent cloudEvent,
                                @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                @Header(KafkaHeaders.OFFSET) long offset,
                                Acknowledgment acknowledgment) {
        
        try {
            System.out.println("\n" + "=".repeat(50));
            System.out.println("Processing CloudEvent from partition " + partition + ", offset " + offset);
            
            // Load schema from Schema Registry if not already loaded
            if (schema == null) {
                loadSchemaFromRegistry();
            }
            
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
                System.err.println("Acknowledging message and continuing...");
                acknowledgment.acknowledge();
                return; // Skip this record
            }
            
            // PROCESS CLOUDEVENT DATA
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
            String sourcePlatform = cloudEvent.getSourceplatform();
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
            
            // Acknowledge the message
            acknowledgment.acknowledge();

        } catch (Exception e) {
            System.err.println("Error processing CloudEvent: " + e.getMessage());
            e.printStackTrace();
            // Acknowledge even on error to avoid infinite reprocessing
            // In production, you might want to implement retry logic or dead letter queue
            acknowledgment.acknowledge();
        }
    }

    private void loadSchemaFromRegistry() {
        try {
            String subject = "person-worker-events-value";
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
            
            this.schema = registrySchema;
            
        } catch (Exception e) {
            System.err.println("Failed to load CloudEvent schema from Schema Registry: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Cannot continue without CloudEvent schema", e);
        }
    }

    private boolean isPersonWorkerEvent(String eventType) {
        return eventType.contains("person") || eventType.contains("PersonWorker");
    }

    private boolean isOrderEvent(String eventType) {
        return eventType.contains("order") || eventType.contains("Order");
    }

    private boolean isInventoryEvent(String eventType) {
        return eventType.contains("inventory") || eventType.contains("Inventory");
    }

    private void processPersonWorkerEvent(CloudEvent cloudEvent, String clientId) {
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

    private void handlePersonWorkerCreated(PersonWorkerData personData, CloudEvent cloudEvent, String clientId) {
        System.out.println("Handling PersonWorker CREATED event");
        System.out.printf("Client %s: Creating new person %s %s (GUPI: %s)%n", 
            clientId, personData.getFirstName(), personData.getLastName(), personData.getGupi());
        
        String sourcePlatform = cloudEvent.getSourceplatform();
        String sourcePlatformId = cloudEvent.getSourceplatformid();
        System.out.printf("Source: %s (Platform ID: %s)%n", sourcePlatform, sourcePlatformId);
        
        System.out.println("Saving new person to database...");
        System.out.println("Sending welcome email...");
        System.out.println("Triggering onboarding workflow...");
        System.out.println("PersonWorker creation processed successfully");
    }

    private void handlePersonWorkerUpdated(PersonWorkerData personData, CloudEvent cloudEvent, String clientId) {
        System.out.println("Handling PersonWorker UPDATED event");
        System.out.printf("Client %s: Updating person %s %s (GUPI: %s)%n", 
            clientId, personData.getFirstName(), personData.getLastName(), personData.getGupi());
        
        String sourcePlatform = cloudEvent.getSourceplatform();
        String sourcePlatformId = cloudEvent.getSourceplatformid();
        System.out.printf("Source: %s (Platform ID: %s)%n", sourcePlatform, sourcePlatformId);
        
        System.out.println("Updating person record in database...");
        System.out.println("Triggering change notifications...");
        System.out.println("Updating related systems...");
        System.out.println("PersonWorker update processed successfully");
    }

    private void handlePersonWorkerDeleted(PersonWorkerData personData, CloudEvent cloudEvent, String clientId) {
        System.out.println("Handling PersonWorker DELETED event");
        System.out.printf("Client %s: Deleting person %s %s (GUPI: %s)%n", 
            clientId, personData.getFirstName(), personData.getLastName(), personData.getGupi());
        
        String sourcePlatform = cloudEvent.getSourceplatform();
        String sourcePlatformId = cloudEvent.getSourceplatformid();
        System.out.printf("Source: %s (Platform ID: %s)%n", sourcePlatform, sourcePlatformId);
        
        System.out.println("Soft deleting person record...");
        System.out.println("Archiving related data...");
        System.out.println("Triggering cleanup processes...");
        System.out.println("PersonWorker deletion processed successfully");
    }

    private void processOrderEvent(CloudEvent cloudEvent, String clientId) {
        System.out.println("Processing Order CloudEvent...");
        System.out.printf("Client %s: Order event received but not yet implemented%n", clientId);
        System.out.println("Event Type: " + cloudEvent.getType());
        System.out.println("Source Platform: " + cloudEvent.getSourceplatform());
    }

    private void processInventoryEvent(CloudEvent cloudEvent, String clientId) {
        System.out.println("Processing Inventory CloudEvent...");
        System.out.printf("Client %s: Inventory event received but not yet implemented%n", clientId);
        System.out.println("Event Type: " + cloudEvent.getType());
        System.out.println("Source Platform: " + cloudEvent.getSourceplatform());
    }

    private void logUnhandledEvent(CloudEvent cloudEvent) {
        System.out.println("Logging unhandled CloudEvent details:");
        System.out.println("  Type: " + cloudEvent.getType());
        System.out.println("  Source: " + cloudEvent.getSource());
        System.out.println("  Source Platform: " + cloudEvent.getSourceplatform());
        System.out.println("  Source Platform ID: " + cloudEvent.getSourceplatformid());
        System.out.println("  Client ID: " + cloudEvent.getClientid());
        System.out.println("  Spec Version: " + cloudEvent.getSpecversion());
    }

    private GenericRecord convertToGenericRecord(CloudEvent cloudEvent, Schema schema) {
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

    private GenericRecord convertDataToGenericRecord(Object data, Schema schema) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        
        try {
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
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert data to GenericRecord. Data type: " + 
                data.getClass().getName() + ", Schema: " + schema.getName(), e);
        }
        
        return builder.build();
    }
}