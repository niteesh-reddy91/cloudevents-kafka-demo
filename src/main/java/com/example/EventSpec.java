package com.example;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.net.URI;
import java.time.OffsetDateTime;

public class EventSpec<T> {
    private String id;
    private String type;
    private URI source;
    private OffsetDateTime time;
    private String subject;
    private String contentType;
    private String clientId;
    private String schemaName;
    private String platformId;
    private String topic;
    private String subtopic;
    private String extractionTimestamp;
    private String referenceId;
    private String bodyVersion;
    private T data;

    public EventSpec() {}

    public EventSpec(String id, String type, URI source, OffsetDateTime time, String subject, String contentType,
                     String clientId, String schemaName, String platformId,
                     String topic, String subtopic, String extractionTimestamp,
                     String referenceId, String bodyVersion, T data) {
        this.id = id;
        this.type = type;
        this.source = source;
        this.time = time;
        this.subject = subject;
        this.contentType = contentType;
        this.clientId = clientId;
        this.schemaName = schemaName;
        this.platformId = platformId;
        this.topic = topic;
        this.subtopic = subtopic;
        this.extractionTimestamp = extractionTimestamp;
        this.referenceId = referenceId;
        this.bodyVersion = bodyVersion;
        this.data = data;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public URI getSource() { return source; }
    public void setSource(URI source) { this.source = source; }

    public OffsetDateTime getTime() { return time; }
    public void setTime(OffsetDateTime time) { this.time = time; }

    public String getSubject() { return subject; }
    public void setSubject(String subject) { this.subject = subject; }

    public String getContentType() { return contentType; }
    public void setContentType(String contentType) { this.contentType = contentType; }

    public String getClientId() { return clientId; }
    public void setClientId(String clientId) { this.clientId = clientId; }

    public String getSchemaName() { return schemaName; }
    public void setSchemaName(String schemaName) { this.schemaName = schemaName; }

    public String getPlatformId() { return platformId; }
    public void setPlatformId(String platformId) { this.platformId = platformId; }

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }

    public String getSubtopic() { return subtopic; }
    public void setSubtopic(String subtopic) { this.subtopic = subtopic; }

    public String getExtractionTimestamp() { return extractionTimestamp; }
    public void setExtractionTimestamp(String extractionTimestamp) { this.extractionTimestamp = extractionTimestamp; }

    public String getReferenceId() { return referenceId; }
    public void setReferenceId(String referenceId) { this.referenceId = referenceId; }

    public String getBodyVersion() { return bodyVersion; }
    public void setBodyVersion(String bodyVersion) { this.bodyVersion = bodyVersion; }

    public T getData() { return data; }
    public void setData(T data) { this.data = data; }

    // Validation logic
    public boolean isValid() {
        return clientId != null && !clientId.isBlank() &&
               schemaName != null && !schemaName.isBlank() &&
               data != null;
    }

    // Convert to CloudEvent for producing
    public CloudEvent toCloudEvent() {
        byte[] dataBytes = null;
        if (data != null) {
            try {
                ObjectMapper mapper = new ObjectMapper();
                dataBytes = mapper.writeValueAsBytes(data);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                dataBytes = null;
            }
        }

        return CloudEventBuilder.v1()
            .withId(id)
            .withType(type)
            .withSource(source)
            .withTime(time)
            .withSubject(subject)
            .withDataContentType(contentType)
            .withExtension("alightclientid", clientId)
            .withExtension("alightschemaname", schemaName)
            .withExtension("alightplatforminternalid", platformId)
            .withExtension("alighttopic", topic)
            .withExtension("alightsubtopic", subtopic)
            .withExtension("alightsourcesystemextractiontimestamp", extractionTimestamp)
            .withExtension("alightsourcesystemreferenceid", referenceId)
            .withExtension("alightbodyversion", bodyVersion)
            .withData(dataBytes)
            .build();
    }
}
