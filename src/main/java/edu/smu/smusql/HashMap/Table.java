package edu.smu.smusql.HashMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Table {

    private HashMap<Integer, HashMap<String, Object>> table;
    private int nextRowKey = 1;
    private List<String> originalColumnOrder;

    public Table(List<String> columnNames) {
        this.originalColumnOrder = columnNames;
        HashMap<String, Object> columns = new HashMap<>();
        for (String columnName : columnNames) {
            columns.put(columnName, null);
        }

        // Initialize the table with an empty structure
        this.table = new HashMap<>();
        this.table.put(0, columns);
    }

    public List<String> getColumnOrder() {
        return originalColumnOrder;
    }

    public HashMap<Integer, HashMap<String, Object>> getTable() {
        return table;
    }

    public HashMap<String, Object> getRow(int rowId) {
        return table.get(rowId);
    }

    public int getNextRowKey() {
        return nextRowKey++;
    }

    public void addRow(int newRowId, HashMap<String, Object> newRow) {
        //int newRowId = this.table.size()+1;
        table.put(newRowId, newRow);
    }

    public List<HashMap<String, Object>> selectRow(String whereConditionColumn, String whereOperator, Object whereValue, String secondCondition, String secondConditionColumn, String secondOperator, Object secondValue) {

        List<HashMap<String, Object>> result = new ArrayList<>();
        for (HashMap<String, Object> row : table.values()) {
            if (matchCondition(row, whereConditionColumn, whereOperator, whereValue, secondCondition, secondConditionColumn, secondOperator, secondValue)) {
                result.add(row);
            }
        }
        return result;
    }

    public int updateRow(HashMap<String, Object> updates, String whereConditionColumn, String whereOperator, Object whereValue, String secondCondition, String secondConditionColumn, String secondOperator, Object secondValue) {
        
        int updatedCount = 0;
        
        for (HashMap<String, Object> row : table.values()) {
            if (matchCondition(row, whereConditionColumn, whereOperator, whereValue, secondCondition,secondConditionColumn, secondOperator, secondValue)) {
                row.putAll(updates);
                updatedCount++;
            }
        }
        return updatedCount;
    }

    public int deleteRow(String whereConditionColumn, String whereOperator, Object whereValue, String secondCondition, String secondConditionColumn, String secondOperator, Object secondValue) {
        
        int initialSize = table.size();
        
        table.entrySet().removeIf(entry -> matchCondition(entry.getValue(), whereConditionColumn, whereOperator, whereValue, secondCondition, secondConditionColumn, secondOperator, secondValue));

        return initialSize - table.size();
    }

    public HashMap<Integer, Object> getColumnData(String columnName) {
        HashMap<Integer, Object> columnData = new HashMap<>();
        for (Map.Entry<Integer, HashMap<String, Object>> rowEntry : table.entrySet()) {
            columnData.put(rowEntry.getKey(), rowEntry.getValue().get(columnName));
        }
        return columnData;
    }

    //helper method

    private boolean matchCondition(HashMap<String, Object> row,
    String whereConditionColumn, String whereOperator, Object whereValue,
    String secondCondition, String secondConditionColumn, String secondOperator, Object secondValue) {

        // Evaluate the first condition
        boolean firstCondition = evaluateCondition(row, whereConditionColumn, whereOperator, whereValue);

        // If there's no second condition, return the result of the first condition
        if (secondCondition == null || secondConditionColumn == null || secondOperator == null || secondValue == null) {
        return firstCondition;
        }

        // Evaluate the second condition
        boolean secondConditionResult = evaluateCondition(row, secondConditionColumn, secondOperator, secondValue);

        // Apply AND/OR logic
        if ("AND".equalsIgnoreCase(secondCondition)) {
        return firstCondition && secondConditionResult;
        } else if ("OR".equalsIgnoreCase(secondCondition)) {
        return firstCondition || secondConditionResult;
        } else {
        throw new IllegalArgumentException("Invalid logical operator: " + secondCondition);
        }
        }

    // Helper method to evaluate a single condition
    private boolean evaluateCondition(HashMap<String, Object> row, String column, String operator, Object value) {
        Object columnValue = row.get(column);

        // Handle null column value cases
        if (columnValue == null) {
        return false;
        }

        // Check for type compatibility
        if (columnValue instanceof Comparable && value instanceof Comparable) {
        Comparable<Object> compColumnValue = (Comparable<Object>) columnValue;
        Comparable<Object> compValue = (Comparable<Object>) value;

        switch (operator) {
        case "=":
        return compColumnValue.equals(compValue);
        case "<":
        return compColumnValue.compareTo(compValue) < 0;
        case ">":
        return compColumnValue.compareTo(compValue) > 0;
        case "<=":
        return compColumnValue.compareTo(compValue) <= 0;
        case ">=":
        return compColumnValue.compareTo(compValue) >= 0;
        default:
        throw new IllegalArgumentException("Invalid operator: " + operator);
        }
        } else {
        throw new IllegalArgumentException("Type mismatch or incompatible types for comparison.");
        }
        }
    // private String formatRow(Object[] row) {
    //     StringBuilder rowString = new StringBuilder("[");
    //     for (int i = 0; i < row.length; i++) {
    //         rowString.append(columnOrder.get(i)).append(": ").append(row[i]);
    //         if (i < row.length - 1) rowString.append(", ");
    //     }
    //     rowString.append("]");
    //     return rowString.toString();
    // }

}