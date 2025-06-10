package com.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericEnumSymbol;
import java.util.*;
import java.util.regex.Pattern;

public class AvroDataValidator {
    
    public static class ValidationException extends Exception {
        private List<String> errors;
        
        public ValidationException(List<String> errors) {
            super("Validation failed: " + String.join(", ", errors));
            this.errors = errors;
        }
        
        public List<String> getErrors() { return errors; }
    }
    
    public static void validate(GenericRecord record, Schema schema) throws ValidationException {
        List<ValidationRuleParser.ValidationRule> rules = ValidationRuleParser.parseValidationRules(schema);
        List<String> errors = new ArrayList<>();
        
        for (ValidationRuleParser.ValidationRule rule : rules) {
            Object fieldValue = getNestedFieldValue(record, rule.getFieldPath());
            
            if (!validateField(fieldValue, rule, record)) {
                errors.add(rule.getMessage());
            }
        }
        
        if (!errors.isEmpty()) {
            throw new ValidationException(errors);
        }
    }
    
    private static Object getNestedFieldValue(GenericRecord record, String fieldPath) {
        String[] pathParts = fieldPath.split("\\.");
        Object currentValue = record;
        
        for (String part : pathParts) {
            if (currentValue instanceof GenericRecord) {
                currentValue = ((GenericRecord) currentValue).get(part);
            } else {
                return null; // Path doesn't exist
            }
        }
        
        return currentValue;
    }
    
    private static boolean validateField(Object value, ValidationRuleParser.ValidationRule rule, GenericRecord rootRecord) {
        switch (rule.getRuleType()) {
            case "required":
                return value != null;
                
            case "recordType":
                // Validate that the nested record is of the expected type
                if (value instanceof GenericRecord) {
                    GenericRecord nestedRecord = (GenericRecord) value;
                    String actualType = nestedRecord.getSchema().getName();
                    String expectedType = rule.getRuleValue().toString();
                    return expectedType.equals(actualType);
                }
                return value == null; // null is allowed unless there's a separate required rule
                
            case "min":
                if (value instanceof Number) {
                    double val = ((Number) value).doubleValue();
                    double min = ((Number) rule.getRuleValue()).doubleValue();
                    return val >= min;
                }
                break;
                
            case "max":
                if (value instanceof Number) {
                    double val = ((Number) value).doubleValue();
                    double max = ((Number) rule.getRuleValue()).doubleValue();
                    return val <= max;
                }
                break;
                
            case "minLength":
                if (value instanceof CharSequence) {
                    return value.toString().length() >= (Integer) rule.getRuleValue();
                }
                break;
                
            case "maxLength":
                if (value instanceof CharSequence) {
                    return value.toString().length() <= (Integer) rule.getRuleValue();
                }
                break;
                
            case "pattern":
                if (value instanceof CharSequence) {
                    Pattern pattern = Pattern.compile(rule.getRuleValue().toString());
                    return pattern.matcher(value.toString()).matches();
                }
                return value == null; // null values pass pattern validation unless required
                
            case "allowedValues":
                if (value != null) {
                    if (rule.getRuleValue() instanceof List) {
                        List<?> allowedValues = (List<?>) rule.getRuleValue();
                        String valueStr = value.toString();
                        return allowedValues.contains(valueStr);
                    }
                }
                return true; // null values pass unless there's a required rule
                
            case "fixedSize":
                if (value instanceof GenericFixed) {
                    GenericFixed fixedValue = (GenericFixed) value;
                    int expectedSize = (Integer) rule.getRuleValue();
                    return fixedValue.bytes().length == expectedSize;
                } else if (value instanceof byte[]) {
                    byte[] byteArray = (byte[]) value;
                    int expectedSize = (Integer) rule.getRuleValue();
                    return byteArray.length == expectedSize;
                }
                break;
                
            case "format":
                if (value instanceof CharSequence) {
                    String format = rule.getRuleValue().toString();
                    return validateFormat(value.toString(), format);
                }
                return value == null; // null values pass format validation unless required
                
            case "notEmpty":
                if (value instanceof CharSequence) {
                    return !value.toString().trim().isEmpty();
                }
                return value != null; // For non-string types, just check not null
                
            default:
                // Handle custom validation rules
                if (rule.getRuleType().startsWith("custom:")) {
                    String customType = rule.getRuleType().substring(7); // Remove "custom:" prefix
                    return validateCustomRule(value, customType, rule.getRuleValue());
                }
                
                // Unknown rule type - log warning and pass validation
                System.out.println("Unknown validation rule type: " + rule.getRuleType());
                return true;
        }
        
        return value == null; // For unhandled cases, null passes unless required
    }
    
    private static boolean validateFormat(String value, String format) {
        switch (format.toLowerCase()) {
            case "email":
                return value.matches("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$");
            case "url":
                return value.matches("^(https?|ftp)://[^\\s/$.?#].[^\\s]*$");
            case "uuid":
                return value.matches("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
            case "date":
                return value.matches("^\\d{4}-\\d{2}-\\d{2}$");
            case "datetime":
                return value.matches("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(.\\d{3})?Z?$");
            case "alphanumeric":
                return value.matches("^[a-zA-Z0-9]+$");
            case "numeric":
                return value.matches("^\\d+$");
            default:
                System.out.println("Unknown format type: " + format);
                return true; // Unknown format, pass validation
        }
    }
    
    private static boolean validateCustomRule(Object value, String customType, Object ruleValue) {
        // This method can be extended for custom validation logic
        // For now, we'll just log and pass
        System.out.println("Custom validation rule not implemented: " + customType);
        return true;
    }
}