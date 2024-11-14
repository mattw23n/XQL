package edu.smu.smusql.ArrayList;

public class ConditionBuilder {

    // Build a condition dynamically
    public static Condition buildCondition(String columnName, String operator, Object value, Table table) {
        return row -> {
            String columnValue = (String) row.getColumn(table.getColumnIndex(columnName)); // Always a String
            Object convertedColumnValue = convertToBestType(columnValue);
            Object convertedValue = convertToBestType(value.toString());

            // For Boolean comparison
            if (convertedColumnValue instanceof Boolean) {
                return handleBooleanComparison((Boolean) convertedColumnValue, operator, convertedValue);
            }

            // For Comparable types (Integer, Double, String)
            if (convertedColumnValue instanceof Comparable) {
                return applyOperator((Comparable) convertedColumnValue, operator, convertedValue);
            } else {
                throw new IllegalArgumentException("Column " + columnName + " is not comparable.");
            }
        };
    }

    private static Object convertToBestType(String value) {
        // Check for Boolean
        if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
            return Boolean.parseBoolean(value);
        }
    
        // Check for Double (converting all numbers to Double)
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException ignored) {
            // Not a Double
        }
    
        // Fallback to String if no other type matched
        return value;
    }
    

    // // Attempt to convert the String value to the best possible type (Integer, Double, Boolean, or String)
    // private static Object convertToBestType(String value) {
    //     // Check for Boolean
    //     if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
    //         return Boolean.parseBoolean(value);
    //     }
    //     // Check for Integer
    //     try {
    //         return Integer.parseInt(value);
    //     } catch (NumberFormatException ignored) {
    //         // Not an Integer
    //     }
    //     // Check for Double
    //     try {
    //         return Double.parseDouble(value);
    //     } catch (NumberFormatException ignored) {
    //         // Not a Double
    //     }
    //     // Fallback to String if no other type matched
    //     return value;
    // }

    // Handle comparison for Boolean values
    private static boolean handleBooleanComparison(Boolean columnValue, String operator, Object value) {
        if (!(value instanceof Boolean)) {
            throw new IllegalArgumentException("Invalid comparison between Boolean and non-Boolean value");
        }

        Boolean booleanValue = (Boolean) value;
        switch (operator) {
            case "=":
                return columnValue.equals(booleanValue);
            case "!=":
                return !columnValue.equals(booleanValue);
            default:
                throw new IllegalArgumentException("Unsupported operator for Boolean: " + operator);
        }
    }

    // Generic method to apply the operator for Comparable values
    private static <T extends Comparable<T>> boolean applyOperator(T columnValue, String operator, Object value) {
        T comparableValue = (T) value; // Safe cast to Comparable type
        switch (operator) {
            case "=":
                return columnValue.compareTo(comparableValue) == 0;
            case "!=":
                return columnValue.compareTo(comparableValue) != 0;
            case ">":
                return columnValue.compareTo(comparableValue) > 0;
            case "<":
                return columnValue.compareTo(comparableValue) < 0;
            case ">=":
                return columnValue.compareTo(comparableValue) >= 0;
            case "<=":
                return columnValue.compareTo(comparableValue) <= 0;
            default:
                throw new IllegalArgumentException("Unsupported operator: " + operator);
        }
    }
}

// public class ConditionBuilder {
//     // Build a dynamic condition based on the column, operator, and value
//     public static Condition buildCondition(String columnName, String operator, Object value, Table table) {
//         return row -> {
//             Object columnValue = row.getColumn(table.getColumnIndex(columnName));
//             if (columnValue instanceof Comparable) {
//                 Comparable comparableValue = (Comparable) columnValue;

//                 // Apply the operator
//                 switch (operator) {
//                     case ">":
//                         return comparableValue.compareTo(value) > 0;
//                     case "<":
//                         return comparableValue.compareTo(value) < 0;
//                     case ">=":
//                         return comparableValue.compareTo(value) >= 0;
//                     case "<=":
//                         return comparableValue.compareTo(value) <= 0;
//                     case "=":
//                         return comparableValue.equals(value);
//                     case "!=":
//                         return !comparableValue.equals(value);
//                     default:
//                         throw new IllegalArgumentException("Unsupported operator: " + operator);
//                 }
//             } else {
//                 throw new IllegalArgumentException("Column " + columnName + " is not comparable.");
//             }
//         };
//     }
// }