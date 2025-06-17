package com.example.config;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Contact;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.info.License;
import io.swagger.v3.oas.annotations.servers.Server;
import org.springframework.context.annotation.Configuration;

@Configuration
@OpenAPIDefinition(
    info = @Info(
        title = "CloudEvents PersonWorker REST API",
        version = "1.0.0",
        description = """
            REST API for PersonWorker lifecycle management using CloudEvents and Kafka.
            
            This API provides synchronous HTTP endpoints that trigger asynchronous CloudEvent 
            processing via Kafka. Each operation validates data against Avro schemas and 
            publishes events to the event streaming platform.
            
            **Related Documentation:**
            - AsyncAPI Specification: Documents the event-driven architecture
            - Avro Schema: Defines data validation rules in src/main/avro/combined.avsc
            
            **Event Flow:**
            1. REST API receives request
            2. Data validation against request schema
            3. CloudEvent creation and publishing to Kafka
            4. Asynchronous processing by downstream consumers
            """,
        contact = @Contact(
            name = "Data Platform Team",
            email = "dataplatform@example.com"
        ),
        license = @License(
            name = "Apache 2.0",
            url = "https://www.apache.org/licenses/LICENSE-2.0"
        )
    ),
    servers = {
        @Server(url = "http://localhost:8080", description = "Local development server"),
        @Server(url = "https://api-dev.example.com", description = "Development server"),
        @Server(url = "https://api.example.com", description = "Production server")
    }
)
public class OpenApiConfig {
}