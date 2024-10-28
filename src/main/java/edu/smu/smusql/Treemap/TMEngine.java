package edu.smu.smusql.Treemap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import edu.smu.smusql.CustomParser;
import edu.smu.smusql.Engine;

import java.util.Map;

public class TMEngine extends Engine {
    private TreeMap<String, Table> database = new TreeMap<String, Table>();
    HashMap<String, Object> parsedSQL;
    String tableName;
    Table table;
    private static String name = "TreeMap";
    private static String[][] stats = { { "completed in 30 seconds" }, { "traverses rows first, then columns" } };

    public TMEngine() {
        super(name, stats);
    }

    public String executeSQL(String query) {
        try {
            parsedSQL = CustomParser.parseSQL(query);

            tableName = (String) parsedSQL.get("tableName");

            if (!"create".equalsIgnoreCase((String) parsedSQL.get("command"))) {
                table = database.get(tableName.toLowerCase());
                if (table == null) {
                    return "Error: no such table: " + tableName;
                }
            }

        } catch (Exception e) {
            return e.getMessage();
        }

        String command = (String) parsedSQL.get("command");

        switch (command) {
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

    public String insert(HashMap<String, Object> parsedSql) {
        List<String> values = (List<String>) parsedSQL.get("columns");

        // check if number of columns and number of values trying to insert is the same
        if (values.size() != table.getTable().get(0).keySet().size()) {
            return "ERROR: Column count doesn't match value count";
        }

        // create a new treemap of columns
        TreeMap<String, Object> newRow = new TreeMap<>();
        int i = 0;
        // insert each value into the respective column
        for (String columnName : table.getColumnOrder()) {
            newRow.put(columnName, values.get(i));
            i++;
        }

        // add this new row of column values into the table
        table.addRow(newRow); // Add the new row to the table

        return "Row inserted into " + tableName;
    }

    public String delete(HashMap<String, Object> parsedSQL) {
        boolean rowDeleted = false; // Track if any row was deleted
        List<Integer> rowsToDelete = new ArrayList<>();
        // Collect keys to delete later because if u delete as u loop. For loop breaks
        // Iterate over rows to delete
        for (int i : table.getTable().keySet()) {
            TreeMap<String, Object> row = table.getTable().get(i);

            if (i == 0)
                continue; // Skip header row

            boolean matchesCondition = evaluateWhereClause(row);
            if (matchesCondition) {
                rowsToDelete.add(i);
            }
        }

        // Remove the rows marked for deletion
        for (Integer rowNum : rowsToDelete) {
            table.getTable().remove(rowNum);
            rowDeleted = true;
        }

        return rowDeleted ? "Rows deleted from " + tableName : "No rows matched the WHERE condition";
    }

    public String select(HashMap<String, Object> parsedSQL) {

        // check if target is String or array (* or specific columns)
        if (parsedSQL.get("target").getClass().equals(String.class)) {
            return selectAllRows();
        } else {
            return selectSpecificColumns();
        }
    }

    private String selectAllRows() {
        StringBuilder result = new StringBuilder();
        result.append("Result:\n");

        // Print header (column names)
        if (!table.getTable().isEmpty()) {
            TreeMap<String, Object> firstRow = table.getTable().firstEntry().getValue();
            result.append(String.join("\t", firstRow.keySet())).append("\n");
        }

        // Iterate over each row
        for (Integer rowEntry : table.getTable().keySet()) {
            if (rowEntry == 0) {
                continue; // Skip header row
            }

            TreeMap<String, Object> row = table.getTable().get(rowEntry);

            // Apply WHERE clause filtering (if present)
            if (parsedSQL.containsKey("whereConditionColumn") && !evaluateWhereClause(row)) {
                continue; // Skip rows that don't satisfy WHERE condition
            }

            // Print the row values
            for (Object value : row.values()) {
                result.append(value).append("\t"); // Convert Object to String
            }
            result.append("\n");
        }

        return result.toString();
    }

    private String selectSpecificColumns() {
        StringBuilder result = new StringBuilder();
        result.append("Result:\n");

        // Print header (column names)
        if (!table.getTable().isEmpty()) {
            TreeMap<String, Object> firstRow = table.getTable().firstEntry().getValue();
            result.append(String.join("\t", firstRow.keySet())).append("\n");
        }

        // Iterate over each row
        for (int i : table.getTable().keySet()) {
            TreeMap<String, Object> row = table.getTable().get(i);
            if (i == 0) {
                continue; // Skip null row
            }

            // Apply WHERE clause filtering (if present)
            if (parsedSQL.containsKey("whereConditionColumn") && !evaluateWhereClause(row)) {
                continue; // Skip rows that don't satisfy WHERE condition
            }

            List<String> selectedColumns = (List<String>) parsedSQL.get("target");
            // Print the values for the selected columns
            for (String column : selectedColumns) {
                if (row.containsKey(column)) {
                    Object value = row.get(column);
                    result.append(value != null ? value.toString() : "NULL").append("\t"); // Convert Object to String
                } else {
                    result.append("NULL").append("\t"); // Column not found
                }
            }
            result.append("\n");
        }

        return result.toString();
    }

    private static boolean evaluateCondition(TreeMap<String, Object> row, String columnName, String operator,
            String expectedValue) {
        Double colVal = 0.0;
        Double expVal = 0.0;

        if (!operator.equals(operator) && row.containsKey(columnName)) {
            colVal = Double.parseDouble(row.get(columnName).toString());
            expVal = Double.parseDouble(expectedValue);
        }
        switch (operator) {
            case "=":
                // Evaluate the condition
                if (row.containsKey(columnName) && row.get(columnName).toString().equals(expectedValue)) {
                    return true; // OR condition is satisfied
                }
                break;
            case "<":
                if (colVal < expVal) {
                    return true;
                }
                break;
            case ">":
                if (colVal > expVal) {
                    return true;
                }
                break;
            default:
                return false;
        }
        return false;
    }

    private boolean evaluateWhereClause(TreeMap<String, Object> row) {
        String columnName = (String) parsedSQL.get("whereConditionColumn");
        String operator = (String) parsedSQL.get("whereOperator");
        String expectedValue = (String) parsedSQL.get("whereValue");

        // Evaluate the WHERE condition
        boolean whereResult = evaluateCondition(row, columnName, operator, expectedValue);

        // evaluate second condition
        boolean secondResult = false;
        if (parsedSQL.containsKey("secondCondition")) {
            String secondCondition = (String) parsedSQL.get("secondCondition");
            columnName = (String) parsedSQL.get("secondConditionColumn");
            operator = (String) parsedSQL.get("secondOperator");
            expectedValue = (String) parsedSQL.get("secondValue");
            secondResult = evaluateCondition(row, columnName, operator, expectedValue);

            if (secondCondition.equalsIgnoreCase("and")) {
                whereResult &= secondResult; // Combine AND conditions
            } else if (secondCondition.equalsIgnoreCase("or")) {
                whereResult |= secondResult; // Combine or conditions
            }
        }
        return whereResult;
    }

    public String update(HashMap<String, Object> parsedSQL) {
        TreeMap<String, String> updates = new TreeMap<>();

        List<String> toUpdate = (List<String>) parsedSQL.get("target");

        // Loop through columns to update
        for (int i = 0; i < toUpdate.size(); i += 2) {
            updates.put(toUpdate.get(i), toUpdate.get(i + 1)); // Store new value
        }

        boolean rowUpdated = false; // Track updates

        // Iterate over rows to update
        for (int i : table.getTable().keySet()) {
            TreeMap<String, Object> row = table.getTable().get(i);
            if (i == 0)
                continue; // Skip header

            boolean matchesCondition = evaluateWhereClause(row);

            if (matchesCondition) {
                // Apply updates to this row
                for (String key : updates.keySet()) {
                    if (row.containsKey(key)) {
                        row.put(key, updates.get(key));
                        rowUpdated = true; // Mark that a row was updated
                    } else {
                        System.out.println("Column " + key + " does not exist in table " + tableName);
                    }
                }
            }
        }

        return rowUpdated ? "Rows updated in " + tableName : "No rows matched the WHERE condition";
    }

    public String create(HashMap<String, Object> parsedSQL) {

        List<String> columns = (List<String>) parsedSQL.get("columns");

        database.put(tableName, new Table(columns));

        return "TM " + tableName + " created";
    }

}
