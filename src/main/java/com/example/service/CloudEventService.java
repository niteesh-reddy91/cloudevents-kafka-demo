package com.example.service;

import com.example.CloudEvent;
import com.example.PersonWorkerData;
import com.example.AvroDataValidator;
import com.example.ValidationRuleParser;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Value;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.github.springwolf.core.asyncapi.annotations.AsyncPublisher;
import io.github.springwolf.core.asyncapi.annotations.AsyncOperation;
import org.springframework.messaging.handler.annotation.Payload;

import java.io.File;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
public class CloudEventService {

    private final KafkaTemplate<String, CloudEvent> kafkaTemplate;
    private final SchemaRegistryClient schemaRegistryClient;
    private final Schema cloudEventSchema;
    
    @Value("${app.kafka.topic.person-worker-events:person-worker-events}")
    private String personWorkerTopic;

    @Autowired
    public CloudEventService(KafkaTemplate<String, CloudEvent> kafkaTemplate, 
                           SchemaRegistryClient schemaRegistryClient) {
        this.kafkaTemplate = kafkaTemplate;
        this.schemaRegistryClient = schemaRegistryClient;
        this.cloudEventSchema = loadSchemaFromFile();
    }

    @AsyncPublisher(operation = @AsyncOperation(
        channelName = "person-worker-events",
        description = "Publishes a CloudEvent when a PersonWorker is created in the system"
    ))
    public String publishPersonWorkerCreated(@Payload PersonWorkerData personData, String clientId) throws Exception {
        CloudEvent cloudEvent = createCloudEvent(
            "com.example.person.created",
            "/prod/user-service/container-123",
            "Workday",
            "aaa111",
            clientId,
            personData
        );
        
        return publishCloudEvent(cloudEvent);
    }

    @AsyncPublisher(operation = @AsyncOperation(
        channelName = "person-worker-events",
        description = "Publishes a CloudEvent when a PersonWorker is updated in the system"
    ))
    public String publishPersonWorkerUpdated(@Payload PersonWorkerData personData, String clientId) throws Exception {
        CloudEvent cloudEvent = createCloudEvent(
            "com.example.person.updated", 
            "/prod/user-service/container-456",
            "SAP",
            "bbb222",
            clientId,
            personData
        );
        
        return publishCloudEvent(cloudEvent);
    }

    @AsyncPublisher(operation = @AsyncOperation(
        channelName = "person-worker-events",
        description = "Publishes a CloudEvent when a PersonWorker is deleted from the system"
    ))
    public String publishPersonWorkerDeleted(@Payload PersonWorkerData personData, String clientId) throws Exception {
        CloudEvent cloudEvent = createCloudEvent(
            "com.example.person.deleted",
            "/prod/user-service/container-789", 
            "Oracle",
            "ccc333",
            clientId,
            personData
        );
        
        return publishCloudEvent(cloudEvent);
    }

    private CloudEvent createCloudEvent(String type, String source, String sourceplatform,
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

    private String publishCloudEvent(CloudEvent cloudEvent) throws Exception {
        // Validate before sending
        validateCloudEvent(cloudEvent);
        
        // Send to Kafka
        CompletableFuture<SendResult<String, CloudEvent>> future = 
            kafkaTemplate.send(personWorkerTopic, cloudEvent);
            
        // Wait for result and return event ID
        SendResult<String, CloudEvent> result = future.get();
        System.out.println("Successfully published CloudEvent with ID: " + cloudEvent.getId() +
                         " to partition: " + result.getRecordMetadata().partition() +
                         " offset: " + result.getRecordMetadata().offset());
        
        return cloudEvent.getId();
    }

    private void validateCloudEvent(CloudEvent cloudEvent) throws AvroDataValidator.ValidationException {
        System.out.println("Validating CloudEvent using validation rules from avsc file...");
        GenericRecord cloudEventRecord = convertToGenericRecord(cloudEvent, cloudEventSchema);
        AvroDataValidator.validate(cloudEventRecord, cloudEventSchema);
        System.out.println("Validation passed - CloudEvent is valid according to validation rules");
    }

    private GenericRecord convertToGenericRecord(CloudEvent cloudEvent, Schema schema) {
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

    private Schema loadSchemaFromFile() {
        try {
            File schemaFile = new File("src/main/avro/combined.avsc");
            if (!schemaFile.exists()) {
                throw new IOException("Schema file not found: combined.avsc");
            }
            
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
                            return unionSchema;
                        }
                    }
                } else if (elementSchema.getType() == Schema.Type.RECORD && 
                          "CloudEvent".equals(elementSchema.getName())) {
                    return elementSchema;
                }
            }
            
            // If it's a single record schema
            if (schema.getType() == Schema.Type.RECORD && "CloudEvent".equals(schema.getName())) {
                return schema;
            }
            
            // If it's a union, look for CloudEvent
            if (schema.getType() == Schema.Type.UNION) {
                for (Schema unionSchema : schema.getTypes()) {
                    if (unionSchema.getType() == Schema.Type.RECORD && 
                        "CloudEvent".equals(unionSchema.getName())) {
                        return unionSchema;
                    }
                }
            }
            
            throw new IOException("Could not find CloudEvent record in schema file");
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to load CloudEvent schema", e);
        }
    }
}