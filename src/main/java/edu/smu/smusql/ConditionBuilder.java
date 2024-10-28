package edu.smu.smusql;

public class ConditionBuilder {

    // Build a condition dynamically
    public static Condition buildCondition(String columnName, String operator, Object value, Table table) {
        return row -> {
            Object columnValue = row.getColumn(table.getColumnIndex(columnName));

            // Check if the value is Boolean and handle separately
            if (columnValue instanceof Boolean) {
                return handleBooleanComparison((Boolean) columnValue, operator, value);
            }

            // For all other Comparable types
            if (columnValue instanceof Comparable) {
                Comparable comparableValue = (Comparable) columnValue;

                // Apply the operator
                switch (operator) {
                    case "=":
                        return comparableValue.compareTo(value) == 0;
                    case "!=":
                        return comparableValue.compareTo(value) != 0;
                    case ">":
                        return comparableValue.compareTo(value) > 0;
                    case "<":
                        return comparableValue.compareTo(value) < 0;
                    case ">=":
                        return comparableValue.compareTo(value) >= 0;
                    case "<=":
                        return comparableValue.compareTo(value) <= 0;
                    default:
                        throw new IllegalArgumentException("Unsupported operator: " + operator);
                }
            } else {
                throw new IllegalArgumentException("Column " + columnName + " is not comparable.");
            }
        };
    }

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