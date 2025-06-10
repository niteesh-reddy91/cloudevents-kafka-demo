package com.example;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.jackson.JsonFormat;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Properties;
import java.util.UUID;
import java.util.Map;

public class App {
    private static final String AVRO_SCHEMA_FILE = "combined.avsc";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    public static void main(String[] args) throws Exception {
        String topic = "demo-topic";
        String schemaRegistryUrl = System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081");

        // Create Schema Registry client
        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 1000);
        
        // Load the schema from the avsc file
        Schema schema = loadSchemaFromFile(AVRO_SCHEMA_FILE);
        
        // Extract validation rules from the schema
        ValidationRuleParser ruleParser = new ValidationRuleParser();
        Map<String, Object> validationRules = ruleParser.extractValidationRules(schema);
        
        System.out.println("Loaded schema from file: " + AVRO_SCHEMA_FILE);
        System.out.println("Extracted validation rules: " + validationRules.keySet());
        
        // Create sample data - PersonWorker
        PersonWorker person = new PersonWorker();
        person.setFirstName("John");
        person.setLastName("Doe");
        person.setAge(30);
        person.setEmail("john.doe@example.com");
        person.setStatus("active");
        // Set the required fields based on your PersonWorker implementation
        person.setUuid(UUID.randomUUID().toString());
        person.setSex(com.example.Sex.MALE); // or FEMALE, OTHER
        person.setBirthdate("1993-06-09"); // example birthdate
        
        // Create CloudEvent using the SDK with Avro serialization instead of Jackson
        CloudEvent cloudEvent = createCloudEventWithAvro(
            "com.example.PersonWorker.Created",
            "user-service",
            "/example/udp/personworker",
            person
        );
        
        // Convert CloudEvent to our Avro envelope structure
        CloudEventEnvelope envelope = convertCloudEventToEnvelope(cloudEvent, "person-worker-events");

        // VALIDATE BEFORE SENDING
        try {
            System.out.println("Validating data using validation rules from avsc file...");
            GenericRecord envelopeRecord = convertToGenericRecord(envelope, schema);
            AvroDataValidator.validate(envelopeRecord, schema);
            System.out.println("Validation passed - data is valid according to validation rules");
        } catch (AvroDataValidator.ValidationException e) {
            System.err.println("Validation failed:");
            for (String error : e.getErrors()) {
                System.err.println("  - " + error);
            }
            return; // Don't send invalid data
        }

        // Send to Kafka
        sendToKafka(envelope, topic, schemaRegistryUrl, schemaRegistryClient);
        
        // Example with different domain model (if you had a Client class)
        // demonstrateGenericUsage(topic, schemaRegistryUrl, schemaRegistryClient, schema);
    }
    
    /**
     * Create CloudEvent using Avro's built-in JSON serialization instead of Jackson
     * This avoids the schema serialization issue with Avro-generated classes
     */
    public static CloudEvent createCloudEventWithAvro(String type, String source, String subject, Object data) {
        try {
            String dataJson;
            
            // Check if data is an Avro-generated object
            if (data instanceof org.apache.avro.specific.SpecificRecord) {
                // Use Avro's native JSON serialization
                dataJson = serializeAvroObjectToJson((org.apache.avro.specific.SpecificRecord) data);
            } else {
                // Fallback to Jackson for regular POJOs
                dataJson = objectMapper.writeValueAsString(data);
            }
            
            return CloudEventBuilder.v1()
                    .withId(UUID.randomUUID().toString())
                    .withType(type)
                    .withSource(URI.create(source))
                    .withSubject(subject)
                    .withTime(OffsetDateTime.now())
                    .withData("application/json", dataJson.getBytes())
                    .withExtension("clientid", UUID.randomUUID().toString())
                    .build();
                    
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize data to JSON", e);
        }
    }
    
    /**
     * Serialize Avro object to JSON using Avro's native JSON encoder
     * This properly handles Avro schemas without including metadata fields
     */
    private static String serializeAvroObjectToJson(org.apache.avro.specific.SpecificRecord avroRecord) throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        
        // Use Avro's JsonEncoder to convert the record to JSON
        JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(avroRecord.getSchema(), outputStream);
        SpecificDatumWriter<org.apache.avro.specific.SpecificRecord> writer = 
            new SpecificDatumWriter<>(avroRecord.getSchema());
        
        writer.write(avroRecord, jsonEncoder);
        jsonEncoder.flush();
        
        return outputStream.toString();
    }
    
    /**
     * Generic method to create CloudEvent for any data type (kept for backwards compatibility)
     * Now uses the new Avro-aware method
     */
    public static <T> CloudEvent createCloudEvent(String type, String source, String subject, T data) {
        return createCloudEventWithAvro(type, source, subject, data);
    }
    
    /**
     * Convert CloudEvent to our custom Avro envelope structure
     * Updated to use Avro's built-in JSON deserialization
     */
    private static CloudEventEnvelope convertCloudEventToEnvelope(CloudEvent cloudEvent, String topic) {
        CloudEventEnvelope envelope = new CloudEventEnvelope();
        envelope.setId(cloudEvent.getId());
        envelope.setType(cloudEvent.getType());
        envelope.setSource(cloudEvent.getSource().toString());
        envelope.setSubject(cloudEvent.getSubject());
        envelope.setTime(cloudEvent.getTime().toString());
        envelope.setClientid(cloudEvent.getExtension("clientid").toString());
        envelope.setTopic(topic);
        
        // Deserialize the Avro JSON data back to the appropriate object using Avro's JSON decoder
        try {
            if (cloudEvent.getData() != null) {
                String dataJson = new String(cloudEvent.getData().toBytes());
                
                // Determine the data type based on the CloudEvent type
                if (cloudEvent.getType().contains("PersonWorker")) {
                    PersonWorker personData = deserializeJsonToAvroObject(dataJson, PersonWorker.class);
                    envelope.setData(personData);
                }
                // Add more data types here as needed:
                // else if (cloudEvent.getType().contains("Client")) {
                //     Client clientData = deserializeJsonToAvroObject(dataJson, Client.class);
                //     envelope.setData(clientData);
                // }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize CloudEvent data", e);
        }
        
        return envelope;
    }
    
    /**
     * Deserialize JSON string back to Avro object using Avro's built-in JSON decoder
     * This replaces the manual Jackson parsing approach
     */
    private static <T extends org.apache.avro.specific.SpecificRecord> T deserializeJsonToAvroObject(
            String jsonString, Class<T> clazz) throws Exception {
        
        try {
            // Create a new instance of the target class to get its schema
            T instance = clazz.getDeclaredConstructor().newInstance();
            Schema schema = instance.getSchema();
            
            // Use Avro's JsonDecoder to parse the JSON
            JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(schema, jsonString);
            
            // Use SpecificDatumReader to deserialize into the specific Avro class
            SpecificDatumReader<T> reader = new SpecificDatumReader<>(schema);
            T result = reader.read(null, jsonDecoder);
            
            return result;
            
        } catch (Exception e) {
            throw new Exception("Failed to deserialize JSON to Avro object of type " + clazz.getName() + 
                              ". JSON: " + jsonString + ". Error: " + e.getMessage(), e);
        }
    }
    
    /**
     * Generic method to send any CloudEvent to Kafka
     */
    public static <T> void sendCloudEventToKafka(String type, String source, String subject, 
                                                T data, String topic, String kafkaBootstrapServers, 
                                                String schemaRegistryUrl) throws Exception {
        // Create CloudEvent using Avro serialization
        CloudEvent cloudEvent = createCloudEventWithAvro(type, source, subject, data);
        
        // Convert to envelope
        CloudEventEnvelope envelope = convertCloudEventToEnvelope(cloudEvent, topic);
        
        // Load schema and validate
        Schema schema = loadSchemaFromFile(AVRO_SCHEMA_FILE);
        GenericRecord envelopeRecord = convertToGenericRecord(envelope, schema);
        AvroDataValidator.validate(envelopeRecord, schema);
        
        // Send to Kafka
        CachedSchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 1000);
        sendToKafka(envelope, topic, schemaRegistryUrl, schemaRegistryClient);
    }
    
    private static void sendToKafka(CloudEventEnvelope envelope, String topic, 
                                   String schemaRegistryUrl, SchemaRegistryClient schemaRegistryClient) {
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
        System.out.println("KafkaAvroSerializer will auto-register/update schema for topic: " + topic);

        try (KafkaProducer<String, CloudEventEnvelope> producer = new KafkaProducer<>(props)) {
            System.out.println("Producer created successfully, sending message...");
            
            ProducerRecord<String, CloudEventEnvelope> record = new ProducerRecord<>(topic, envelope);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Failed to send message: " + exception.getMessage());
                    exception.printStackTrace();
                } else {
                    System.out.println("Sent validated CloudEventEnvelope to Kafka topic " + topic);
                    System.out.println("Partition: " + metadata.partition() + ", Offset: " + metadata.offset());
                }
            });

            producer.flush();
            System.out.println("Message sent and flushed successfully");
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
     * Load schema from the avsc file
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
            
            // If it's an array schema (multiple records), find the CloudEventEnvelope record
            if (schema.getType() == Schema.Type.ARRAY) {
                Schema elementSchema = schema.getElementType();
                if (elementSchema.getType() == Schema.Type.UNION) {
                    // Handle union types in array
                    for (Schema unionSchema : elementSchema.getTypes()) {
                        if (unionSchema.getType() == Schema.Type.RECORD && 
                            "CloudEventEnvelope".equals(unionSchema.getName())) {
                            System.out.println("Found CloudEventEnvelope schema in array union");
                            return unionSchema;
                        }
                    }
                } else if (elementSchema.getType() == Schema.Type.RECORD && 
                          "CloudEventEnvelope".equals(elementSchema.getName())) {
                    System.out.println("Found CloudEventEnvelope schema in array");
                    return elementSchema;
                }
            }
            
            // If it's a single record schema
            if (schema.getType() == Schema.Type.RECORD && "CloudEventEnvelope".equals(schema.getName())) {
                System.out.println("Found CloudEventEnvelope schema as single record");
                return schema;
            }
            
            // If it's a union, look for CloudEventEnvelope
            if (schema.getType() == Schema.Type.UNION) {
                for (Schema unionSchema : schema.getTypes()) {
                    if (unionSchema.getType() == Schema.Type.RECORD && 
                        "CloudEventEnvelope".equals(unionSchema.getName())) {
                        System.out.println("Found CloudEventEnvelope schema in union");
                        return unionSchema;
                    }
                }
            }
            
            throw new IOException("Could not find CloudEventEnvelope record in schema file: " + filename);
            
        } catch (Exception e) {
            throw new IOException("Failed to parse schema file: " + filename + ". Error: " + e.getMessage(), e);
        }
    }
    
    private static GenericRecord convertToGenericRecord(CloudEventEnvelope envelope, Schema schema) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set("id", envelope.getId());
        builder.set("source", envelope.getSource());
        builder.set("type", envelope.getType());
        builder.set("subject", envelope.getSubject());
        builder.set("time", envelope.getTime());
        builder.set("clientid", envelope.getClientid());
        builder.set("topic", envelope.getTopic());
        
        // Convert data field (supports different domain models)
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
     * Generic method to convert any domain object to GenericRecord
     * This method handles different domain models dynamically
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
                // Fallback to Jackson for regular POJOs
                Map<String, Object> dataMap = objectMapper.convertValue(data, Map.class);
                
                for (Schema.Field field : schema.getFields()) {
                    String fieldName = field.name();
                    Object fieldValue = dataMap.get(fieldName);
                    
                    if (fieldValue != null) {
                        builder.set(fieldName, fieldValue);
                    } else if (field.hasDefaultValue()) {
                        builder.set(fieldName, field.defaultVal());
                    }
                }
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
    
    /**
     * Example method showing how to use this with different domain models
     */
    public static void demonstrateGenericUsage(String topic, String schemaRegistryUrl, 
                                              SchemaRegistryClient schemaRegistryClient, Schema schema) {
        try {
            // Example 1: PersonWorker
            PersonWorker person = new PersonWorker();
            person.setFirstName("Jane");
            person.setLastName("Smith");
            person.setAge(25);
            person.setEmail("jane.smith@example.com");
            person.setStatus("active");
            person.setUuid(UUID.randomUUID().toString());
            person.setSex(com.example.Sex.FEMALE);
            person.setBirthdate("1993-06-09"); // example birthdate
            
            CloudEvent personEvent = createCloudEvent(
                "com.example.PersonWorker.Updated",
                "user-service", 
                "/users/person-worker",
                person
            );
            
            System.out.println("Created CloudEvent for PersonWorker: " + personEvent.getId());
            
            // If you had other domain models, you could use the same pattern:
            // Client client = new Client("ACME Corp", "enterprise");
            // CloudEvent clientEvent = createCloudEvent(
            //     "com.example.Client.Created",
            //     "client-service",
            //     "/clients/enterprise", 
            //     client
            // );
            
        } catch (Exception e) {
            System.err.println("Error in demonstration: " + e.getMessage());
            e.printStackTrace();
        }
    }
}