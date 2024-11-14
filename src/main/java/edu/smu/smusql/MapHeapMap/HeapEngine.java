package edu.smu.smusql.MapHeapMap;

import edu.smu.smusql.CustomParser;
import edu.smu.smusql.Engine;
import java.util.*;

public class HeapEngine extends Engine {
    private Map<String, HeapTable> database = new HashMap<>();
    private HashMap<String, Object> parsedSQL;
    private String tableName;
    private HeapTable table;

    private static String name = "MinHeapSQL";
    private static String[][] stats = { { "completed in 30 seconds" }, { "traverses rows first, then columns" } };

    public HeapEngine() {
        super(name, stats);
    }

    public String executeSQL(String query) {
        try {
            parsedSQL = CustomParser.parseSQL(query);
            tableName = ((String) parsedSQL.get("tableName")).toLowerCase();

            if (!"create".equalsIgnoreCase((String) parsedSQL.get("command"))) {
                table = database.get(tableName);
                if (table == null) {
                    return "Error: no such table: " + tableName;
                }
            }
        } catch (Exception e) {
            return e.getMessage();
        }

        String command = (String) parsedSQL.get("command");

        switch (command.toUpperCase()) {
            case "CREATE":
                return create(parsedSQL);
            case "INSERT":
                return insert(parsedSQL);
            case "SELECT":
                return select(parsedSQL);
            case "UPDATE":
                return update(parsedSQL);
            case "DELETE":
                return delete(parsedSQL);
            default:
                return "ERROR: Unknown command";
        }
    }

    public String create(HashMap<String, Object> parsedSQL) {
        List<String> columns = (List<String>) parsedSQL.get("columns");

        // Create a new table with column names
        database.put(tableName, new HeapTable(columns));
        return "Table " + tableName + " created.";
    }

    public String insert(HashMap<String, Object> parsedSQL) {
        List<String> values = (List<String>) parsedSQL.get("columns");

        if (values.size() != table.getColumnOrder().size()) {
            return "ERROR: Column count doesn't match value count.";
        }

        Map<String, Object> newRow = new HashMap<>();
        int i = 0;
        // insert each value into the respective column
        for (String columnName : table.getColumnOrder()) {
            newRow.put(columnName, values.get(i));
            i++;
        }

        table.addRow(newRow);
        return "Row inserted into " + tableName;
    }

    public String delete(HashMap<String, Object> parsedSQL) {
        boolean rowDeleted = false;
        List<Integer> rowsToDelete = new ArrayList<>();

        // Collect rows that match the condition for deletion
        for (MinHeap.Entry<Integer, Map<String, Object>> entry : table.getTable().getRowsInTable()) {
            Map<String, Object> row = entry.getValue();

            if (evaluateWhereClause(row)) {
                rowsToDelete.add(entry.getKey());
            }
        }

        // Delete rows
        for (Integer rowKey : rowsToDelete) {
            table.getTable().deleteByID(rowKey);
            rowDeleted = true;
        }

        return rowDeleted ? "Rows deleted from " + tableName : "No rows matched the WHERE condition.";
    }

    public String select(HashMap<String, Object> parsedSQL) {
        StringBuilder result = new StringBuilder("Result:\n");

        // Select specific columns or all columns (*)
        List<String> selectedColumns = parsedSQL.get("target").equals("*") ? 
                                       table.getColumnOrder() : (List<String>) parsedSQL.get("target");

        result.append(String.join("\t", selectedColumns)).append("\n");

        for (MinHeap.Entry<Integer, Map<String, Object>> entry : table.getTable().getRowsInTable()) {
            Map<String, Object> row = entry.getValue();

            if (parsedSQL.containsKey("whereConditionColumn") && !evaluateWhereClause(row)) {
                continue;
            }

            for (String column : selectedColumns) {
                Object value = row.getOrDefault(column, "NULL");
                result.append(value).append("\t");
            }
            result.append("\n");
        }

        return result.toString();
    }

    public String update(HashMap<String, Object> parsedSQL) {
        List<String> toUpdate = (List<String>) parsedSQL.get("target");
        Map<String, Object> updates = new HashMap<>();

        for (int i = 0; i < toUpdate.size(); i += 2) {
            updates.put(toUpdate.get(i), toUpdate.get(i + 1));
        }

        boolean rowUpdated = false;
        for (MinHeap.Entry<Integer, Map<String, Object>> entry : table.getTable().getRowsInTable()) {
            Map<String, Object> row = entry.getValue();

            if (evaluateWhereClause(row)) {
                for (Map.Entry<String, Object> update : updates.entrySet()) {
                    if (row.containsKey(update.getKey())) {
                        row.put(update.getKey(), update.getValue());
                        rowUpdated = true;
                    }
                }
            }
        }

        return rowUpdated ? "Rows updated in " + tableName : "No rows matched the WHERE condition.";
    }

    private boolean evaluateWhereClause(Map<String, Object> row) {
        String columnName = (String) parsedSQL.get("whereConditionColumn");
        String operator = (String) parsedSQL.get("whereOperator");
        String expectedValue = (String) parsedSQL.get("whereValue");

        boolean whereResult = evaluateCondition(row, columnName, operator, expectedValue);
        if (parsedSQL.containsKey("secondCondition")) {
            String secondCondition = (String) parsedSQL.get("secondCondition");
            String secondColumn = (String) parsedSQL.get("secondConditionColumn");
            String secondOperator = (String) parsedSQL.get("secondOperator");
            String secondValue = (String) parsedSQL.get("secondValue");

            boolean secondResult = evaluateCondition(row, secondColumn, secondOperator, secondValue);
            whereResult = secondCondition.equalsIgnoreCase("AND") ? whereResult && secondResult : whereResult || secondResult;
        }

        return whereResult;
    }

    private boolean evaluateCondition(Map<String, Object> row, String columnName, String operator, String expectedValue) {
        Object columnValue = row.get(columnName);
        if (columnValue == null) return false;

        switch (operator) {
            case "=":
                return columnValue.toString().equals(expectedValue);
            case "<":
                return Double.parseDouble(columnValue.toString()) < Double.parseDouble(expectedValue);
            case ">":
                return Double.parseDouble(columnValue.toString()) > Double.parseDouble(expectedValue);
            case "<=":
                return Double.parseDouble(columnValue.toString()) <= Double.parseDouble(expectedValue);
            case ">=":
                return Double.parseDouble(columnValue.toString()) >= Double.parseDouble(expectedValue);
            default:
                return false;
        }
    }
}
