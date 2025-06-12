package com.example;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.io.File;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Properties;
import java.util.UUID;
import java.util.Map;

public class App {
    private static final String AVRO_SCHEMA_FILE = "combined.avsc";
    
    public static void main(String[] args) throws Exception {
        String topic = "person-worker-events";
        String schemaRegistryUrl = System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081");

        // Create Schema Registry client
        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 1000);
        
        // Load the CloudEvent schema from the avsc file
        Schema cloudEventSchema = loadSchemaFromFile(AVRO_SCHEMA_FILE);
        
        // Extract validation rules from the schema
        ValidationRuleParser ruleParser = new ValidationRuleParser();
        Map<String, Object> validationRules = ruleParser.extractValidationRules(cloudEventSchema);
        
        System.out.println("Loaded CloudEvent schema from file: " + AVRO_SCHEMA_FILE);
        System.out.println("Extracted validation rules: " + validationRules.keySet());
        
        // Create sample PersonWorker data
        PersonWorkerData personData = new PersonWorkerData();
        personData.setGupi("abc123");
        personData.setFirstName("John");
        personData.setMiddleName("Optional");
        personData.setLastName("Doe");
        personData.setSuffix("Jr.");
        personData.setPrefix("Mr.");
        personData.setBirthDate("1970-01-01");
        personData.setDeceasedDate("2020-01-01");
        personData.setSex("M");
        personData.setLegalName("Mr. John O. Doe Jr.");
        personData.setMaritalStatus("Single");
        
        // Create CloudEvent directly as Avro record
        CloudEvent cloudEvent = createCloudEventAvro(
            "com.example.person.created",
            "/dev/user-service/container-123",
            "Workday",
            "aaa111",
            "00095",
            personData
        );
        
        // VALIDATE BEFORE SENDING
        try {
            System.out.println("Validating CloudEvent using validation rules from avsc file...");
            GenericRecord cloudEventRecord = convertToGenericRecord(cloudEvent, cloudEventSchema);
            AvroDataValidator.validate(cloudEventRecord, cloudEventSchema);
            System.out.println("Validation passed - CloudEvent is valid according to validation rules");
        } catch (AvroDataValidator.ValidationException e) {
            System.err.println("Validation failed:");
            for (String error : e.getErrors()) {
                System.err.println("  - " + error);
            }
            return; // Don't send invalid data
        }

        // Send to Kafka
        sendCloudEventToKafka(cloudEvent, topic, schemaRegistryUrl, schemaRegistryClient, cloudEventSchema);
    }
    
    /**
     * Create CloudEvent as Avro-generated object
     */
    public static CloudEvent createCloudEventAvro(String type, String source, String sourceplatform, 
                                                 String sourceplatformid, String clientid, PersonWorkerData data) {
        CloudEvent cloudEvent = new CloudEvent();
        cloudEvent.setSpecversion("1.0");
        cloudEvent.setId(UUID.randomUUID().toString());
        cloudEvent.setSource(source);
        cloudEvent.setType(type);
        cloudEvent.setDatacontenttype("application/avro");
        cloudEvent.setTime(OffsetDateTime.now().toString());
        cloudEvent.setSourceplatform(sourceplatform);
        cloudEvent.setSourceplatformid(sourceplatformid);
        cloudEvent.setClientid(clientid);
        cloudEvent.setData(data);
        
        return cloudEvent;
    }
    
    /**
     * Generic method to create CloudEvent for any data type
     */
    public static <T> CloudEvent createCloudEvent(String type, String source, String sourceplatform,
                                                 String sourceplatformid, String clientid, T data) {
        CloudEvent cloudEvent = new CloudEvent();
        cloudEvent.setSpecversion("1.0");
        cloudEvent.setId(UUID.randomUUID().toString());
        cloudEvent.setSource(source);
        cloudEvent.setType(type);
        cloudEvent.setDatacontenttype("application/avro");
        cloudEvent.setTime(OffsetDateTime.now().toString());
        cloudEvent.setSourceplatform(sourceplatform);
        cloudEvent.setSourceplatformid(sourceplatformid);
        cloudEvent.setClientid(clientid);
        
        // Handle different data types - you'll need to cast appropriately based on your domain models
        if (data instanceof PersonWorkerData) {
            cloudEvent.setData((PersonWorkerData) data);
        } else {
            // Add other data type handling as needed
            throw new IllegalArgumentException("Unsupported data type: " + data.getClass().getName());
        }
        
        return cloudEvent;
    }
    
    /**
     * Generic method to send any CloudEvent to Kafka with validation
     */
    public static <T> void sendCloudEventToKafka(String type, String source, String sourceplatform,
                                                String sourceplatformid, String clientid, T data, 
                                                String topic, String kafkaBootstrapServers, 
                                                String schemaRegistryUrl) throws Exception {
        // Create CloudEvent
        CloudEvent cloudEvent = createCloudEvent(type, source, sourceplatform, sourceplatformid, clientid, data);
        
        // Load schema and validate
        Schema schema = loadSchemaFromFile(AVRO_SCHEMA_FILE);
        GenericRecord cloudEventRecord = convertToGenericRecord(cloudEvent, schema);
        AvroDataValidator.validate(cloudEventRecord, schema);
        
        // Send to Kafka
        CachedSchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 1000);
        sendCloudEventToKafka(cloudEvent, topic, schemaRegistryUrl, schemaRegistryClient, schema);
    }
    
    private static void sendCloudEventToKafka(CloudEvent cloudEvent, String topic, 
                                            String schemaRegistryUrl, SchemaRegistryClient schemaRegistryClient,
                                            Schema schema) {
        // Producer properties for Avro + Schema Registry
        Properties props = new Properties();
        props.put("bootstrap.servers", System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", schemaRegistryUrl);

        // Add timeout and retry configurations
        props.put("max.block.ms", 120000);
        props.put("request.timeout.ms", 30000);
        props.put("retries", 5);
        props.put("retry.backoff.ms", 1000);
        props.put("delivery.timeout.ms", 120000);
        props.put("client.id", "cloudevent-producer");
        props.put("enable.idempotence", true);

        System.out.println("Connecting to Kafka at: " + props.get("bootstrap.servers"));
        System.out.println("Schema Registry at: " + props.get("schema.registry.url"));
        System.out.println("KafkaAvroSerializer will auto-register/update CloudEvent schema for topic: " + topic);

        try (KafkaProducer<String, CloudEvent> producer = new KafkaProducer<>(props)) {
            System.out.println("Producer created successfully, sending CloudEvent...");
            
            ProducerRecord<String, CloudEvent> record = new ProducerRecord<>(topic, cloudEvent);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Failed to send CloudEvent: " + exception.getMessage());
                    exception.printStackTrace();
                } else {
                    System.out.println("Sent validated CloudEvent to Kafka topic " + topic);
                    System.out.println("CloudEvent ID: " + cloudEvent.getId());
                    System.out.println("CloudEvent Type: " + cloudEvent.getType());
                    System.out.println("Partition: " + metadata.partition() + ", Offset: " + metadata.offset());
                }
            });

            producer.flush();
            System.out.println("CloudEvent sent and flushed successfully");
        } catch (Exception e) {
            System.err.println("Error creating or using producer: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Verify the schema was registered/updated with validation rules
            try {
                String subject = topic + "-value";
                System.out.println("Verifying schema registration for subject: " + subject);
                var latestSchema = schemaRegistryClient.getLatestSchemaMetadata(subject);
                System.out.println("Schema registered with version: " + latestSchema.getVersion());
                System.out.println("Schema ID: " + latestSchema.getId());
                
                // Verify validation rules are preserved in Schema Registry
                Schema registeredSchema = new Schema.Parser().parse(latestSchema.getSchema());
                ValidationRuleParser registryRuleParser = new ValidationRuleParser();
                Map<String, Object> registeredRules = registryRuleParser.extractValidationRules(registeredSchema);
                
                if (!registeredRules.isEmpty()) {
                    System.out.println("✓ Validation rules successfully preserved in Schema Registry");
                    System.out.println("Registered validation rules: " + registeredRules.keySet());
                } else {
                    System.out.println("⚠ Warning: No validation rules found in registered schema");
                }
                
            } catch (Exception e) {
                System.err.println("Could not verify schema registration: " + e.getMessage());
            }
        }
    }

    /**
     * Load CloudEvent schema from the avsc file
     */
    private static Schema loadSchemaFromFile(String filename) throws IOException {
        File schemaFile = new File("src/main/avro/" + filename);
        if (!schemaFile.exists()) {
            throw new IOException("Schema file not found: " + filename + 
                ". Please ensure the file exists in the project root or provide the correct path.");
        }
        
        try {
            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(schemaFile);
            
            // If it's an array schema (multiple records), find the CloudEvent record
            if (schema.getType() == Schema.Type.ARRAY) {
                Schema elementSchema = schema.getElementType();
                if (elementSchema.getType() == Schema.Type.UNION) {
                    // Handle union types in array
                    for (Schema unionSchema : elementSchema.getTypes()) {
                        if (unionSchema.getType() == Schema.Type.RECORD && 
                            "CloudEvent".equals(unionSchema.getName())) {
                            System.out.println("Found CloudEvent schema in array union");
                            return unionSchema;
                        }
                    }
                } else if (elementSchema.getType() == Schema.Type.RECORD && 
                          "CloudEvent".equals(elementSchema.getName())) {
                    System.out.println("Found CloudEvent schema in array");
                    return elementSchema;
                }
            }
            
            // If it's a single record schema
            if (schema.getType() == Schema.Type.RECORD && "CloudEvent".equals(schema.getName())) {
                System.out.println("Found CloudEvent schema as single record");
                return schema;
            }
            
            // If it's a union, look for CloudEvent
            if (schema.getType() == Schema.Type.UNION) {
                for (Schema unionSchema : schema.getTypes()) {
                    if (unionSchema.getType() == Schema.Type.RECORD && 
                        "CloudEvent".equals(unionSchema.getName())) {
                        System.out.println("Found CloudEvent schema in union");
                        return unionSchema;
                    }
                }
            }
            
            throw new IOException("Could not find CloudEvent record in schema file: " + filename);
            
        } catch (Exception e) {
            throw new IOException("Failed to parse schema file: " + filename + ". Error: " + e.getMessage(), e);
        }
    }
    
    /**
     * Convert CloudEvent to GenericRecord for validation
     */
    private static GenericRecord convertToGenericRecord(CloudEvent cloudEvent, Schema schema) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
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
            // Handle PersonWorkerData specifically
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
    
    /**
     * Example method showing how to use this with different scenarios
     */
    public static void demonstrateUsage(String topic, String schemaRegistryUrl, 
                                       SchemaRegistryClient schemaRegistryClient, Schema schema) {
        try {
            // Example: PersonWorker creation event
            PersonWorkerData personData = new PersonWorkerData();
            personData.setGupi("xyz789");
            personData.setFirstName("Jane");
            personData.setLastName("Smith");
            personData.setBirthDate("1985-03-15");
            personData.setSex("F");
            personData.setMaritalStatus("Married");
            
            CloudEvent personEvent = createCloudEventAvro(
                "com.example.person.updated",
                "/prod/user-service/container-456", 
                "SAP",
                "bbb222",
                "00096",
                personData
            );
            
            System.out.println("Created CloudEvent for PersonWorker: " + personEvent.getId());
            
            // You can add other domain models here in the future
            
        } catch (Exception e) {
            System.err.println("Error in demonstration: " + e.getMessage());
            e.printStackTrace();
        }
    }
}