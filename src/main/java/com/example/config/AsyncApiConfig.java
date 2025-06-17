package com.example.config;

import io.github.springwolf.core.configuration.docket.AsyncApiDocket;
import io.github.springwolf.asyncapi.v3.model.info.Info;
import io.github.springwolf.asyncapi.v3.model.info.Contact;
import io.github.springwolf.asyncapi.v3.model.info.License;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AsyncApiConfig {

    @Bean
    public AsyncApiDocket asyncApiDocket() {
        Info info = Info.builder()
            .title("CloudEvents PersonWorker Event API")
            .version("1.0.0")
            .description("""
                Event-driven API using Kafka with CloudEvents and Avro serialization for PersonWorker domain.
                
                This AsyncAPI specification documents the asynchronous event processing capabilities
                including CloudEvent publishing and consumption, Schema Registry integration,
                and comprehensive validation workflows.
                
                **Key Features:**
                - CloudEvents specification compliance
                - Avro schema validation with custom rules
                - Multi-platform event sourcing (Workday, SAP, Oracle, Salesforce)
                - Real-time event processing with dead letter queue handling
                - Comprehensive business context documentation
                
                **Related Documentation:**
                - REST API: Available at /swagger-ui.html
                - Health Checks: Available at /actuator/health
                """)
            .contact(Contact.builder()
                .name("Data Platform Team")
                .email("dataplatform@example.com")
                .build())
            .license(License.builder()
                .name("Apache 2.0")
                .url("https://www.apache.org/licenses/LICENSE-2.0")
                .build())
            .build();

        return AsyncApiDocket.builder()
            .info(info)
            .build();
    }
}