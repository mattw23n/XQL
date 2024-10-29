package edu.smu.smusql;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

public class Engine {

    private Map<String, MinHeap<Integer, Map<String, String>>> smusql = new HashMap<>();

    public String executeSQL(String query) {
        Map<String, Object> tokens = CustomParser.parseSQL(query);
        String command = (String) tokens.get("command");

        if (command == null) {
            return "ERROR: Command not recognized.";
        }

        switch (command) {
            case "CREATE":
                return create(tokens);
            case "INSERT":
                return insert(tokens);
            case "SELECT":
                return select(tokens);
            case "UPDATE":
                return update(tokens);
            case "DELETE":
                return delete(tokens);
            default:
                return "ERROR: Unknown command";
        }
    }

    public String insert(Map<String, Object> tokens) {
        String tableName = (String) tokens.get("tableName");
        MinHeap<Integer, Map<String, String>> table = smusql.get(tableName);

        if (table == null) {
            return "ERROR: Table " + tableName + " does not exist.";
        }

        // Ensure columns and values are present
        List<String> columns = (List<String>) tokens.get("columns");
        List<String> values = (List<String>) tokens.get("values");

        if (columns == null || values == null || columns.size() != values.size()) {
            return "ERROR: Invalid INSERT statement. Check columns and values.";
        }

        Map<String, String> columnData = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            columnData.put(columns.get(i), values.get(i));
        }

        try {
            int primaryKey = Integer.parseInt(values.get(0));
            table.insert(primaryKey, columnData);
        } catch (NumberFormatException e) {
            return "ERROR: Primary key must be an integer.";
        }

        return "Inserted into " + tableName;
    }

    public String select(Map<String, Object> tokens) {
        String tableName = (String) tokens.get("tableName");
        MinHeap<Integer, Map<String, String>> table = smusql.get(tableName);

        if (table == null) {
            return "ERROR: Table " + tableName + " does not exist.";
        }

        Set<MinHeap.Entry<Integer, Map<String, String>>> results = table.getRowsInTable();
        if (tokens.size() > 3) {
            results = filterResults(results, tokens);
        }

        return formatResults(results);
    }

    public String update(Map<String, Object> tokens) {
        String tableName = (String) tokens.get("tableName");
        MinHeap<Integer, Map<String, String>> table = smusql.get(tableName);

        if (table == null) {
            return "ERROR: Table " + tableName + " does not exist.";
        }

        int rowsUpdated = 0;
        Set<MinHeap.Entry<Integer, Map<String, String>>> rows = table.getRowsInTable();
        for (MinHeap.Entry<Integer, Map<String, String>> row : rows) {
            if (matchesCondition(row.getValue(), tokens)) {
                updateRow(row.getValue(), tokens);
                rowsUpdated++;
            }
        }

        return "Updated " + rowsUpdated + " rows.";
    }

    public String delete(Map<String, Object> tokens) {
        String tableName = (String) tokens.get("tableName");
        MinHeap<Integer, Map<String, String>> table = smusql.get(tableName);

        if (table == null) {
            return "ERROR: Table " + tableName + " does not exist.";
        }

        int rowsDeleted = 0;
        Set<MinHeap.Entry<Integer, Map<String, String>>> rows = table.getRowsInTable();
        for (MinHeap.Entry<Integer, Map<String, String>> row : new HashSet<>(rows)) {
            if (matchesCondition(row.getValue(), tokens)) {
                table.deleteByID(row.getKey());
                rowsDeleted++;
            }
        }

        return "Deleted " + rowsDeleted + " rows.";
    }

    public String create(Map<String, Object> tokens) {
        String tableName = (String) tokens.get("tableName");

        if (tableName == null) {
            return "ERROR: Table name not specified.";
        }

        if (smusql.containsKey(tableName)) {
            return "ERROR: Table " + tableName + " already exists.";
        }

        smusql.put(tableName, new MinHeap<>());
        return "Created table " + tableName;
    }

    private boolean matchesCondition(Map<String, String> row, Map<String, Object> tokens) {
        String whereColumn = (String) tokens.get("whereConditionColumn");
        String operator = (String) tokens.get("whereOperator");
        String whereValue = (String) tokens.get("whereValue");

        if (whereColumn == null || operator == null || whereValue == null) {
            return false;
        }

        String columnValue = row.get(whereColumn);
        return checkOperation(columnValue, operator, whereValue);
    }

    private void updateRow(Map<String, String> row, Map<String, Object> tokens) {
        String updateColumn = (String) tokens.get("updateColumn");
        String updateValue = (String) tokens.get("updateValue");

        if (updateColumn != null && updateValue != null) {
            row.put(updateColumn, updateValue);
        }
    }

    private Set<MinHeap.Entry<Integer, Map<String, String>>> filterResults(
            Set<MinHeap.Entry<Integer, Map<String, String>>> rows, Map<String, Object> tokens) {

        Set<MinHeap.Entry<Integer, Map<String, String>>> filteredRows = new HashSet<>();
        for (MinHeap.Entry<Integer, Map<String, String>> row : rows) {
            if (matchesCondition(row.getValue(), tokens)) {
                filteredRows.add(row);
            }
        }
        return filteredRows;
    }

    private String formatResults(Set<MinHeap.Entry<Integer, Map<String, String>>> results) {
        StringBuilder output = new StringBuilder();
        for (MinHeap.Entry<Integer, Map<String, String>> entry : results) {
            output.append(entry.toString()).append("\n");
        }
        return output.toString();
    }

    public boolean checkOperation(String left, String operator, String right) {
        if (left == null) return false;

        switch (operator) {
            case "=":
                return left.equals(right);
            case "<":
                return left.compareTo(right) < 0;
            case ">":
                return left.compareTo(right) > 0;
            case "<=":
                return left.compareTo(right) <= 0;
            case ">=":
                return left.compareTo(right) >= 0;
            default:
                return false;
        }
    }
}
