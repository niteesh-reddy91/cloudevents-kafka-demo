package com.example;

import org.apache.avro.Schema;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;

public class ValidationRuleParser {
    private static final ObjectMapper mapper = new ObjectMapper();
    
    public static class ValidationRule {
        private String fieldPath; // Can be nested like "data.firstName"
        private String ruleType;
        private Object ruleValue;
        private String message;
        
        public ValidationRule(String fieldPath, String ruleType, Object ruleValue, String message) {
            this.fieldPath = fieldPath;
            this.ruleType = ruleType;
            this.ruleValue = ruleValue;
            this.message = message;
        }
        
        // Getters
        public String getFieldPath() { return fieldPath; }
        public String getRuleType() { return ruleType; }
        public Object getRuleValue() { return ruleValue; }
        public String getMessage() { return message; }
    }
    
    public static List<ValidationRule> parseValidationRules(Schema schema) {
        List<ValidationRule> rules = new ArrayList<>();
        
        try {
            // Get the validationRules property from schema
            String validationRulesJson = schema.getProp("validationRules");
            if (validationRulesJson == null) {
                return rules;
            }
            
            JsonNode rulesNode = mapper.readTree(validationRulesJson);
            
            // Parse all top-level validation rules
            rulesNode.fields().forEachRemaining(entry -> {
                String fieldName = entry.getKey();
                JsonNode fieldRulesNode = entry.getValue();
                
                if ("data".equals(fieldName)) {
                    // Handle special data field with nested structure
                    parseDataFieldRules(fieldRulesNode, rules);
                } else {
                    // Handle regular top-level fields (specversion, id, source, type, etc.)
                    parseFieldRules(fieldName, fieldRulesNode, rules);
                }
            });
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse validation rules", e);
        }
        
        return rules;
    }
    
    private static void parseDataFieldRules(JsonNode dataRules, List<ValidationRule> rules) {
        // Handle data-level rules
        if (dataRules.has("notNullAllowed") && !dataRules.get("notNullAllowed").asBoolean()) {
            rules.add(new ValidationRule("data", "required", true, "Data field cannot be null"));
        }
        
        if (dataRules.has("recordType")) {
            String expectedType = dataRules.get("recordType").asText();
            rules.add(new ValidationRule("data", "recordType", expectedType, 
                "Data must be of type: " + expectedType));
        }
        
        // Handle field-level rules within data
        if (dataRules.has("fields")) {
            JsonNode fieldRules = dataRules.get("fields");
            fieldRules.fields().forEachRemaining(entry -> {
                String fieldName = entry.getKey();
                JsonNode fieldRule = entry.getValue();
                
                // For CloudEvent structure, data fields are accessed directly (data.gupi, data.firstName, etc.)
                parseFieldRules("data." + fieldName, fieldRule, rules);
            });
        }
    }
    
    private static void parseFieldRules(String fieldPath, JsonNode fieldRule, List<ValidationRule> rules) {
        // Handle required fields
        if (fieldRule.has("required") && fieldRule.get("required").asBoolean()) {
            rules.add(new ValidationRule(fieldPath, "required", true, 
                "Field " + fieldPath + " is required"));
        }
        
        // Handle nullable fields
        if (fieldRule.has("nullable")) {
            boolean nullable = fieldRule.get("nullable").asBoolean();
            if (!nullable) {
                rules.add(new ValidationRule(fieldPath, "required", true, 
                    "Field " + fieldPath + " cannot be null"));
            }
        }
        
        // Handle string length constraints
        if (fieldRule.has("minLength")) {
            int minLength = fieldRule.get("minLength").asInt();
            rules.add(new ValidationRule(fieldPath, "minLength", minLength, 
                "Field " + fieldPath + " must be at least " + minLength + " characters long"));
        }
        
        if (fieldRule.has("maxLength")) {
            int maxLength = fieldRule.get("maxLength").asInt();
            rules.add(new ValidationRule(fieldPath, "maxLength", maxLength, 
                "Field " + fieldPath + " must be at most " + maxLength + " characters long"));
        }
        
        // Handle min/max for numbers
        if (fieldRule.has("min")) {
            int min = fieldRule.get("min").asInt();
            rules.add(new ValidationRule(fieldPath, "min", min, 
                "Field " + fieldPath + " must be at least " + min));
        }
        
        if (fieldRule.has("max")) {
            int max = fieldRule.get("max").asInt();
            rules.add(new ValidationRule(fieldPath, "max", max, 
                "Field " + fieldPath + " must be at most " + max));
        }
        
        // Handle patterns (regex)
        if (fieldRule.has("pattern")) {
            String pattern = fieldRule.get("pattern").asText();
            rules.add(new ValidationRule(fieldPath, "pattern", pattern, 
                "Field " + fieldPath + " must match the required pattern"));
        }
        
        // Handle allowed values
        if (fieldRule.has("allowedValues")) {
            JsonNode allowedValues = fieldRule.get("allowedValues");
            List<String> values = new ArrayList<>();
            allowedValues.forEach(val -> values.add(val.asText()));
            rules.add(new ValidationRule(fieldPath, "allowedValues", values, 
                "Field " + fieldPath + " must be one of: " + values));
        }
        
        // Handle fixed size (for UUID)
        if (fieldRule.has("size")) {
            int size = fieldRule.get("size").asInt();
            rules.add(new ValidationRule(fieldPath, "fixedSize", size, 
                "Field " + fieldPath + " must be exactly " + size + " bytes"));
        }
        
        // Handle common string formats
        if (fieldRule.has("format")) {
            String format = fieldRule.get("format").asText();
            rules.add(new ValidationRule(fieldPath, "format", format, 
                "Field " + fieldPath + " must be in " + format + " format"));
        }
        
        // Handle not empty validation
        if (fieldRule.has("notEmpty") && fieldRule.get("notEmpty").asBoolean()) {
            rules.add(new ValidationRule(fieldPath, "notEmpty", true, 
                "Field " + fieldPath + " cannot be empty"));
        }
        
        // Handle custom validation (for future extensibility)
        if (fieldRule.has("custom")) {
            JsonNode customRule = fieldRule.get("custom");
            String customType = customRule.get("type").asText();
            Object customValue = extractCustomRuleValue(customRule.get("value"));
            String customMessage = customRule.has("message") ? 
                customRule.get("message").asText() : 
                "Custom validation failed for field: " + fieldPath;
            
            rules.add(new ValidationRule(fieldPath, "custom:" + customType, customValue, customMessage));
        }
    }
    
    private static Object extractCustomRuleValue(JsonNode valueNode) {
        if (valueNode == null) return null;
        if (valueNode.isInt()) return valueNode.asInt();
        if (valueNode.isDouble()) return valueNode.asDouble();
        if (valueNode.isBoolean()) return valueNode.asBoolean();
        if (valueNode.isArray()) {
            List<Object> values = new ArrayList<>();
            for (JsonNode item : valueNode) {
                values.add(extractCustomRuleValue(item));
            }
            return values;
        }
        return valueNode.asText();
    }

    public Map<String, Object> extractValidationRules(Schema schema) {
        Map<String, Object> rules = new HashMap<>();
        
        try {
            // Get the validationRules property from schema
            String validationRulesJson = schema.getProp("validationRules");
            if (validationRulesJson != null) {
                JsonNode rulesNode = mapper.readTree(validationRulesJson);
                
                // Convert JsonNode to Map for easier access
                rulesNode.fields().forEachRemaining(entry -> {
                    String fieldName = entry.getKey();
                    JsonNode fieldRulesNode = entry.getValue();
                    
                    if ("data".equals(fieldName)) {
                        // Handle data field specially - flatten the nested structure
                        Map<String, Object> dataRules = new HashMap<>();
                        
                        // Add top-level data rules
                        fieldRulesNode.fields().forEachRemaining(dataRuleEntry -> {
                            String dataRuleName = dataRuleEntry.getKey();
                            JsonNode dataRuleValue = dataRuleEntry.getValue();
                            
                            if (!"fields".equals(dataRuleName)) {
                                // Add non-field rules directly
                                if (dataRuleValue.isBoolean()) {
                                    dataRules.put(dataRuleName, dataRuleValue.asBoolean());
                                } else if (dataRuleValue.isInt()) {
                                    dataRules.put(dataRuleName, dataRuleValue.asInt());
                                } else if (dataRuleValue.isArray()) {
                                    List<String> values = new ArrayList<>();
                                    dataRuleValue.forEach(val -> values.add(val.asText()));
                                    dataRules.put(dataRuleName, values);
                                } else {
                                    dataRules.put(dataRuleName, dataRuleValue.asText());
                                }
                            }
                        });
                        
                        rules.put(fieldName, dataRules);
                        
                        // Add individual data field rules as separate entries
                        if (fieldRulesNode.has("fields")) {
                            JsonNode fieldsNode = fieldRulesNode.get("fields");
                            fieldsNode.fields().forEachRemaining(fieldEntry -> {
                                String dataFieldName = fieldEntry.getKey();
                                JsonNode dataFieldRules = fieldEntry.getValue();
                                
                                Map<String, Object> fieldRules = new HashMap<>();
                                dataFieldRules.fields().forEachRemaining(ruleEntry -> {
                                    String ruleName = ruleEntry.getKey();
                                    JsonNode ruleValue = ruleEntry.getValue();
                                    
                                    if (ruleValue.isBoolean()) {
                                        fieldRules.put(ruleName, ruleValue.asBoolean());
                                    } else if (ruleValue.isInt()) {
                                        fieldRules.put(ruleName, ruleValue.asInt());
                                    } else if (ruleValue.isArray()) {
                                        List<String> values = new ArrayList<>();
                                        ruleValue.forEach(val -> values.add(val.asText()));
                                        fieldRules.put(ruleName, values);
                                    } else {
                                        fieldRules.put(ruleName, ruleValue.asText());
                                    }
                                });
                                
                                rules.put("data." + dataFieldName, fieldRules);
                            });
                        }
                    } else {
                        // Handle regular CloudEvent fields (specversion, id, source, type, etc.)
                        Map<String, Object> fieldRules = new HashMap<>();
                        fieldRulesNode.fields().forEachRemaining(ruleEntry -> {
                            String ruleName = ruleEntry.getKey();
                            JsonNode ruleValue = ruleEntry.getValue();
                            
                            if (ruleValue.isBoolean()) {
                                fieldRules.put(ruleName, ruleValue.asBoolean());
                            } else if (ruleValue.isInt()) {
                                fieldRules.put(ruleName, ruleValue.asInt());
                            } else if (ruleValue.isArray()) {
                                List<String> values = new ArrayList<>();
                                ruleValue.forEach(val -> values.add(val.asText()));
                                fieldRules.put(ruleName, values);
                            } else {
                                fieldRules.put(ruleName, ruleValue.asText());
                            }
                        });
                        
                        rules.put(fieldName, fieldRules);
                    }
                });
            }
        } catch (Exception e) {
            System.err.println("Failed to extract validation rules: " + e.getMessage());
        }
        
        return rules;
    }
}