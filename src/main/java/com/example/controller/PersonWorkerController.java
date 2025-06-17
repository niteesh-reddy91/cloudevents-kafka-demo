package com.example.controller;

import com.example.service.CloudEventService;
import com.example.PersonWorkerData;
import com.example.CloudEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.validation.annotation.Validated;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;

@RestController
@RequestMapping("/api/person-worker")
@Validated
@Tag(name = "PersonWorker Lifecycle", 
     description = "Operations for managing PersonWorker lifecycle events. Each operation triggers CloudEvent publishing to Kafka for downstream processing.")
public class PersonWorkerController {

    private final CloudEventService cloudEventService;

    @Autowired
    public PersonWorkerController(CloudEventService cloudEventService) {
        this.cloudEventService = cloudEventService;
    }

    @Operation(
        summary = "Create new PersonWorker",
        description = """
            Creates a new PersonWorker record and publishes a CloudEvent of type 
            `com.example.person.created` to the Kafka topic.
            
            **Business Context:**
            - Triggered when a new employee joins the company
            - HR system creates a new worker profile
            - User registration process completes
            
            **Validation:**
            - All required fields validated against request schema
            - Pattern validation for names and identifiers
            - Date format validation (YYYY-MM-DD)
            - Enumerated values validated for sex, maritalStatus, prefix, suffix
            
            **Downstream Impact:**
            - HR systems initiate onboarding workflow
            - Payroll systems set up employee records
            - Access control systems create user accounts
            - Notification services send welcome communications
            """
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200", 
            description = "PersonWorker created successfully",
            content = @Content(
                mediaType = "text/plain",
                schema = @Schema(type = "string", pattern = "^PersonWorker created successfully\\. Event ID: [a-f0-9-]{36}$"),
                examples = @ExampleObject(value = "PersonWorker created successfully. Event ID: 550e8400-e29b-41d4-a716-446655440000")
            )
        ),
        @ApiResponse(
            responseCode = "400", 
            description = "Validation error or invalid request data",
            content = @Content(
                mediaType = "text/plain",
                schema = @Schema(type = "string"),
                examples = {
                    @ExampleObject(name = "validation_error", value = "Failed to create PersonWorker: firstName is required"),
                    @ExampleObject(name = "avro_error", value = "Failed to create PersonWorker: Invalid date format for birthDate")
                }
            )
        ),
        @ApiResponse(
            responseCode = "500", 
            description = "Internal server error",
            content = @Content(
                mediaType = "text/plain",
                schema = @Schema(type = "string"),
                examples = @ExampleObject(value = "Failed to create PersonWorker: Unable to connect to Kafka")
            )
        )
    })
    @PostMapping("/create")
    public ResponseEntity<String> createPersonWorker(@Valid @RequestBody CreatePersonWorkerRequest request) {
        try {
            PersonWorkerData personData = mapToPersonWorkerData(request);
            String eventId = cloudEventService.publishPersonWorkerCreated(personData, request.getClientId());
            return ResponseEntity.ok("PersonWorker created successfully. Event ID: " + eventId);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body("Failed to create PersonWorker: " + e.getMessage());
        }
    }

    @Operation(
        summary = "Update existing PersonWorker",
        description = """
            Updates an existing PersonWorker record and publishes a CloudEvent of type 
            `com.example.person.updated` to the Kafka topic.
            
            **Business Context:**
            - Employee information is modified (contact details, status, etc.)
            - HR system modifies worker profile
            - Self-service profile updates by employees
            
            **Business Rules:**
            - Only active employees can be updated
            - Age modifications require manager approval
            - Email changes trigger verification workflow
            
            **Downstream Impact:**
            - Payroll systems update records
            - Access control systems adjust permissions
            - Notification service sends update confirmations
            """
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200", 
            description = "PersonWorker updated successfully",
            content = @Content(
                mediaType = "text/plain",
                schema = @Schema(type = "string", pattern = "^PersonWorker updated successfully\\. Event ID: [a-f0-9-]{36}$"),
                examples = @ExampleObject(value = "PersonWorker updated successfully. Event ID: 550e8400-e29b-41d4-a716-446655440001")
            )
        ),
        @ApiResponse(
            responseCode = "400", 
            description = "Validation error or invalid request data",
            content = @Content(
                mediaType = "text/plain",
                schema = @Schema(type = "string"),
                examples = @ExampleObject(value = "Failed to update PersonWorker: Invalid GUPI format")
            )
        ),
        @ApiResponse(
            responseCode = "500", 
            description = "Internal server error",
            content = @Content(
                mediaType = "text/plain",
                schema = @Schema(type = "string"),
                examples = @ExampleObject(value = "Failed to update PersonWorker: Schema Registry unavailable")
            )
        )
    })
    @PutMapping("/update")
    public ResponseEntity<String> updatePersonWorker(@Valid @RequestBody UpdatePersonWorkerRequest request) {
        try {
            PersonWorkerData personData = mapToPersonWorkerData(request);
            String eventId = cloudEventService.publishPersonWorkerUpdated(personData, request.getClientId());
            return ResponseEntity.ok("PersonWorker updated successfully. Event ID: " + eventId);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body("Failed to update PersonWorker: " + e.getMessage());
        }
    }

    @Operation(
        summary = "Delete PersonWorker",
        description = """
            Marks a PersonWorker for deletion and publishes a CloudEvent of type 
            `com.example.person.deleted` to the Kafka topic.
            
            **Business Context:**
            - Employee termination or resignation
            - Contract expiration for temporary workers
            - Data privacy compliance (GDPR right to be forgotten)
            
            **Business Rules:**
            - Only HR managers can trigger deletion events
            - Final payroll must be processed before deletion
            - Archive certain data before full deletion
            
            **Security Priority:**
            - Deletion events processed with highest priority (within 5 seconds)
            - Access control systems immediately revoke permissions
            - All changes logged for security auditing
            """
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200", 
            description = "PersonWorker deleted successfully",
            content = @Content(
                mediaType = "text/plain",
                schema = @Schema(type = "string", pattern = "^PersonWorker deleted successfully\\. Event ID: [a-f0-9-]{36}$"),
                examples = @ExampleObject(value = "PersonWorker deleted successfully. Event ID: 550e8400-e29b-41d4-a716-446655440002")
            )
        ),
        @ApiResponse(
            responseCode = "400", 
            description = "Validation error or invalid request data",
            content = @Content(
                mediaType = "text/plain",
                schema = @Schema(type = "string"),
                examples = @ExampleObject(value = "Failed to delete PersonWorker: Missing required field: gupi")
            )
        ),
        @ApiResponse(
            responseCode = "500", 
            description = "Internal server error",
            content = @Content(
                mediaType = "text/plain",
                schema = @Schema(type = "string"),
                examples = @ExampleObject(value = "Failed to delete PersonWorker: Kafka producer error")
            )
        )
    })
    @DeleteMapping("/delete")
    public ResponseEntity<String> deletePersonWorker(@Valid @RequestBody DeletePersonWorkerRequest request) {
        try {
            PersonWorkerData personData = new PersonWorkerData();
            personData.setGupi(request.getGupi());
            personData.setFirstName(request.getFirstName());
            personData.setLastName(request.getLastName());
            
            String eventId = cloudEventService.publishPersonWorkerDeleted(personData, request.getClientId());
            return ResponseEntity.ok("PersonWorker deleted successfully. Event ID: " + eventId);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body("Failed to delete PersonWorker: " + e.getMessage());
        }
    }

    private PersonWorkerData mapToPersonWorkerData(PersonWorkerRequest request) {
        PersonWorkerData personData = new PersonWorkerData();
        personData.setGupi(request.getGupi());
        personData.setFirstName(request.getFirstName());
        personData.setMiddleName(request.getMiddleName());
        personData.setLastName(request.getLastName());
        personData.setSuffix(request.getSuffix());
        personData.setPrefix(request.getPrefix());
        personData.setBirthDate(request.getBirthDate());
        personData.setDeceasedDate(request.getDeceasedDate());
        personData.setSex(request.getSex());
        personData.setLegalName(request.getLegalName());
        personData.setMaritalStatus(request.getMaritalStatus());
        return personData;
    }

    // Base request class
    @Schema(description = "Base PersonWorker data for lifecycle operations")
    public static abstract class PersonWorkerRequest {
        
        @Schema(description = "Global Unique Person Identifier", 
                example = "emp12345", 
                pattern = "^[a-zA-Z0-9-_]+$",
                minLength = 3, 
                maxLength = 20)
        @NotBlank(message = "GUPI is required")
        @Size(min = 3, max = 20)
        @Pattern(regexp = "^[a-zA-Z0-9-_]+$", message = "GUPI can only contain letters, numbers, hyphens, and underscores")
        private String gupi;
        
        @Schema(description = "First name of the person", 
                example = "John",
                pattern = "^[a-zA-Z\\s'-]+$",
                minLength = 2,
                maxLength = 50)
        @NotBlank(message = "First name is required")
        @Size(min = 2, max = 50)
        @Pattern(regexp = "^[a-zA-Z\\s'-]+$", message = "First name can only contain letters, spaces, apostrophes, and hyphens")
        private String firstName;
        
        @Schema(description = "Middle name of the person (optional)", 
                example = "Michael",
                pattern = "^[a-zA-Z\\s'-]*$",
                maxLength = 50)
        @Size(max = 50)
        @Pattern(regexp = "^[a-zA-Z\\s'-]*$", message = "Middle name can only contain letters, spaces, apostrophes, and hyphens")
        private String middleName;
        
        @Schema(description = "Last name of the person", 
                example = "Doe",
                pattern = "^[a-zA-Z\\s'-]+$",
                minLength = 2,
                maxLength = 50)
        @NotBlank(message = "Last name is required")
        @Size(min = 2, max = 50)
        @Pattern(regexp = "^[a-zA-Z\\s'-]+$", message = "Last name can only contain letters, spaces, apostrophes, and hyphens")
        private String lastName;
        
        @Schema(description = "Name suffix", 
                example = "Jr.",
                allowableValues = {"Jr.", "Sr.", "II", "III", "IV", "V"},
                maxLength = 10)
        @Size(max = 10)
        @Pattern(regexp = "^(Jr\\.|Sr\\.|II|III|IV|V)?$", message = "Suffix must be one of: Jr., Sr., II, III, IV, V")
        private String suffix;
        
        @Schema(description = "Name prefix", 
                example = "Mr.",
                allowableValues = {"Mr.", "Mrs.", "Ms.", "Dr.", "Prof."},
                maxLength = 10)
        @Size(max = 10)
        @Pattern(regexp = "^(Mr\\.|Mrs\\.|Ms\\.|Dr\\.|Prof\\.)?$", message = "Prefix must be one of: Mr., Mrs., Ms., Dr., Prof.")
        private String prefix;
        
        @Schema(description = "Birth date in YYYY-MM-DD format", 
                example = "1990-01-15",
                format = "date",
                pattern = "^\\d{4}-\\d{2}-\\d{2}$")
        @NotBlank(message = "Birth date is required")
        @Pattern(regexp = "^\\d{4}-\\d{2}-\\d{2}$", message = "Birth date must be in YYYY-MM-DD format")
        private String birthDate;
        
        @Schema(description = "Deceased date in YYYY-MM-DD format (if applicable)", 
                example = "2023-12-31",
                format = "date",
                pattern = "^\\d{4}-\\d{2}-\\d{2}$")
        @Pattern(regexp = "^\\d{4}-\\d{2}-\\d{2}$", message = "Deceased date must be in YYYY-MM-DD format")
        private String deceasedDate;
        
        @Schema(description = "Sex - M (Male), F (Female), O (Other)", 
                example = "M",
                allowableValues = {"M", "F", "O"})
        @NotBlank(message = "Sex is required")
        @Pattern(regexp = "^[MFO]$", message = "Sex must be one of: M, F, O")
        private String sex;
        
        @Schema(description = "Full legal name", 
                example = "Mr. John Michael Doe Jr.",
                pattern = "^[a-zA-Z\\s'.-]+$",
                maxLength = 200)
        @Size(max = 200)
        @Pattern(regexp = "^[a-zA-Z\\s'.-]+$", message = "Legal name can only contain letters, spaces, apostrophes, periods, and hyphens")
        private String legalName;
        
        @Schema(description = "Marital status", 
                example = "Single",
                allowableValues = {"Single", "Married", "Divorced", "Widowed", "Separated"})
        @Pattern(regexp = "^(Single|Married|Divorced|Widowed|Separated)$", message = "Marital status must be one of: Single, Married, Divorced, Widowed, Separated")
        private String maritalStatus;
        
        @Schema(description = "5-digit client identifier", 
                example = "00095",
                pattern = "^[0-9]{5}$")
        @NotBlank(message = "Client ID is required")
        @Pattern(regexp = "^[0-9]{5}$", message = "Client ID must be exactly 5 digits")
        private String clientId;

        // Getters and setters
        public String getGupi() { return gupi; }
        public void setGupi(String gupi) { this.gupi = gupi; }
        
        public String getFirstName() { return firstName; }
        public void setFirstName(String firstName) { this.firstName = firstName; }
        
        public String getMiddleName() { return middleName; }
        public void setMiddleName(String middleName) { this.middleName = middleName; }
        
        public String getLastName() { return lastName; }
        public void setLastName(String lastName) { this.lastName = lastName; }
        
        public String getSuffix() { return suffix; }
        public void setSuffix(String suffix) { this.suffix = suffix; }
        
        public String getPrefix() { return prefix; }
        public void setPrefix(String prefix) { this.prefix = prefix; }
        
        public String getBirthDate() { return birthDate; }
        public void setBirthDate(String birthDate) { this.birthDate = birthDate; }
        
        public String getDeceasedDate() { return deceasedDate; }
        public void setDeceasedDate(String deceasedDate) { this.deceasedDate = deceasedDate; }
        
        public String getSex() { return sex; }
        public void setSex(String sex) { this.sex = sex; }
        
        public String getLegalName() { return legalName; }
        public void setLegalName(String legalName) { this.legalName = legalName; }
        
        public String getMaritalStatus() { return maritalStatus; }
        public void setMaritalStatus(String maritalStatus) { this.maritalStatus = maritalStatus; }
        
        public String getClientId() { return clientId; }
        public void setClientId(String clientId) { this.clientId = clientId; }
    }

    @Schema(description = "Request to create a new PersonWorker")
    public static class CreatePersonWorkerRequest extends PersonWorkerRequest {
        
        @Schema(description = "Source platform for the create operation", 
                example = "Workday",
                allowableValues = {"Workday", "SAP", "Oracle", "Salesforce"},
                defaultValue = "Workday")
        @Pattern(regexp = "^(Workday|SAP|Oracle|Salesforce)$", message = "Source platform must be one of: Workday, SAP, Oracle, Salesforce")
        private String sourceplatform = "Workday";
        
        @Schema(description = "Platform-specific identifier", 
                example = "aaa111",
                minLength = 3,
                maxLength = 50)
        @Size(min = 3, max = 50, message = "Source platform ID must be between 3 and 50 characters")
        private String sourceplatformid;

        public String getSourceplatform() { return sourceplatform; }
        public void setSourceplatform(String sourceplatform) { this.sourceplatform = sourceplatform; }
        
        public String getSourceplatformid() { return sourceplatformid; }
        public void setSourceplatformid(String sourceplatformid) { this.sourceplatformid = sourceplatformid; }
    }

    @Schema(description = "Request to update an existing PersonWorker")
    public static class UpdatePersonWorkerRequest extends PersonWorkerRequest {
        
        @Schema(description = "Source platform for the update operation", 
                example = "SAP",
                allowableValues = {"Workday", "SAP", "Oracle", "Salesforce"},
                defaultValue = "SAP")
        @Pattern(regexp = "^(Workday|SAP|Oracle|Salesforce)$", message = "Source platform must be one of: Workday, SAP, Oracle, Salesforce")
        private String sourceplatform = "SAP";
        
        @Schema(description = "Platform-specific identifier", 
                example = "bbb222",
                minLength = 3,
                maxLength = 50)
        @Size(min = 3, max = 50, message = "Source platform ID must be between 3 and 50 characters")
        private String sourceplatformid;

        public String getSourceplatform() { return sourceplatform; }
        public void setSourceplatform(String sourceplatform) { this.sourceplatform = sourceplatform; }
        
        public String getSourceplatformid() { return sourceplatformid; }
        public void setSourceplatformid(String sourceplatformid) { this.sourceplatformid = sourceplatformid; }
    }

    @Schema(description = "Request to delete a PersonWorker")
    public static class DeletePersonWorkerRequest {
        
        @Schema(description = "Global Unique Person Identifier", 
                example = "emp12345", 
                pattern = "^[a-zA-Z0-9-_]+$",
                minLength = 3, 
                maxLength = 20)
        @NotBlank(message = "GUPI is required")
        @Size(min = 3, max = 20)
        @Pattern(regexp = "^[a-zA-Z0-9-_]+$", message = "GUPI can only contain letters, numbers, hyphens, and underscores")
        private String gupi;
        
        @Schema(description = "First name of the person", 
                example = "John",
                pattern = "^[a-zA-Z\\s'-]+$",
                minLength = 2,
                maxLength = 50)
        @NotBlank(message = "First name is required")
        @Size(min = 2, max = 50)
        @Pattern(regexp = "^[a-zA-Z\\s'-]+$", message = "First name can only contain letters, spaces, apostrophes, and hyphens")
        private String firstName;
        
        @Schema(description = "Last name of the person", 
                example = "Doe",
                pattern = "^[a-zA-Z\\s'-]+$",
                minLength = 2,
                maxLength = 50)
        @NotBlank(message = "Last name is required")
        @Size(min = 2, max = 50)
        @Pattern(regexp = "^[a-zA-Z\\s'-]+$", message = "Last name can only contain letters, spaces, apostrophes, and hyphens")
        private String lastName;
        
        @Schema(description = "5-digit client identifier", 
                example = "00097",
                pattern = "^[0-9]{5}$")
        @NotBlank(message = "Client ID is required")
        @Pattern(regexp = "^[0-9]{5}$", message = "Client ID must be exactly 5 digits")
        private String clientId;
        
        @Schema(description = "Source platform for the delete operation", 
                example = "Oracle",
                allowableValues = {"Workday", "SAP", "Oracle", "Salesforce"},
                defaultValue = "Oracle")
        @Pattern(regexp = "^(Workday|SAP|Oracle|Salesforce)$", message = "Source platform must be one of: Workday, SAP, Oracle, Salesforce")
        private String sourceplatform = "Oracle";
        
        @Schema(description = "Platform-specific identifier", 
                example = "ccc333",
                minLength = 3,
                maxLength = 50)
        @Size(min = 3, max = 50, message = "Source platform ID must be between 3 and 50 characters")
        private String sourceplatformid;

        public String getGupi() { return gupi; }
        public void setGupi(String gupi) { this.gupi = gupi; }
        
        public String getFirstName() { return firstName; }
        public void setFirstName(String firstName) { this.firstName = firstName; }
        
        public String getLastName() { return lastName; }
        public void setLastName(String lastName) { this.lastName = lastName; }
        
        public String getClientId() { return clientId; }
        public void setClientId(String clientId) { this.clientId = clientId; }
        
        public String getSourceplatform() { return sourceplatform; }
        public void setSourceplatform(String sourceplatform) { this.sourceplatform = sourceplatform; }
        
        public String getSourceplatformid() { return sourceplatformid; }
        public void setSourceplatformid(String sourceplatformid) { this.sourceplatformid = sourceplatformid; }
    }
}