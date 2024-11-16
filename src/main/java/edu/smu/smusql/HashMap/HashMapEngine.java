package edu.smu.smusql.HashMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.smu.smusql.CustomParser;
import edu.smu.smusql.Engine;
 
public class HashMapEngine extends Engine {

    private final Map<String,Table> database = new HashMap<>();
    
    HashMap<String, Object> parsedSQL;

    String tableName;
    Table table;

    private static String name = "HashMap";
    private static String[][] stats = {{"Extremely fast for exact key-value lookups (O(1))", "Handles collisions efficiently with linked lists and red-black trees"}, {"Performance depends on hash function quality", "Less efficient for range queries"}, {"Without Charging: 37.259s", "With Charging: 20.640s (44.6%)"}, {"250 MB"}};
    
    public HashMapEngine() {
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
//System.out.println("Parsed Tokens: " + tokens); 

    public String insert(HashMap<String, Object> parsedSQL) {

        //error
        if (!database.containsKey(tableName)) {
            return "ERROR: Table does not exist";
        }

        List<String> values = (List<String>) parsedSQL.get("columns");

//System.out.println("Insert Columns: " + columns);
//System.out.println("Insert Values: " + values);
// check if number of columns and number of values trying to insert is the same

        if (values.size() != table.getTable().get(0).keySet().size()) {
            return "ERROR: Column count doesn't match value count";
        }

        HashMap<String, Object> newRow = new HashMap<>();
        int i = 0;
        // insert each value into the respective column
        for (String columnName : table.getColumnOrder()) {
            newRow.put(columnName, values.get(i));
            i++;
        }

        int newRowId = table.getNextRowKey();
        // Add the new row to the table
        table.addRow(newRowId, newRow);

        return "Row inserted into " + tableName;
    }

    public String delete(HashMap<String, Object> parsedSQL) {
        // Error if table does not exist
        if (!database.containsKey(tableName)) {
            return "ERROR: Table does not exist";
        }

        Table table = database.get(tableName);

        if (table == null) {
            return "ERROR: Table is not initialized correctly.";
        }

        int deletedRows = table.deleteRow(
            (String) parsedSQL.get("whereConditionColumn"),
            (String) parsedSQL.get("whereOperator"),
            parsedSQL.get("whereValue"),
            (String) parsedSQL.get("secondCondition"),
            (String) parsedSQL.get("secondConditionColumn"),
            (String) parsedSQL.get("secondOperator"),
            parsedSQL.get("secondValue")
        );

        return deletedRows + " rows deleted from " + tableName;
    }

    public String select(HashMap<String, Object> parsedSQL) {
        // Check if the target is a String or List (for SELECT * or specific columns)
        if (parsedSQL.get("target") instanceof String) {
            return selectAllRows();
        } else {
            return selectSpecificColumns();
        }
    }

    private String selectAllRows() {
        StringBuilder result = new StringBuilder();
        result.append("Result:\n");
    
        // Get the ordered list of column names from the table
        List<String> columnOrder = table.getColumnOrder();
    
        // Print header with ordered columns
        for (String column : columnOrder) {
            result.append(column).append("\t");
        }
        result.append("\n");
    
        // Print each row with values aligned according to column order
        for (int rowKey : table.getTable().keySet()) {
            Map<String, Object> row = table.getTable().get(rowKey);
            if (rowKey == 0) continue; // Skip header row if present
    
            // Apply WHERE clause filtering (if present)
            if (parsedSQL.containsKey("whereConditionColumn") && !evaluateWhereClause(row)) continue;
    
            // Print row values in the same column order
            for (String column : columnOrder) {
                Object value = row.getOrDefault(column, "NULL");
                result.append(value.toString()).append("\t");
            }
            result.append("\n");
        }
    
        return result.toString();
    }
    
    private String selectSpecificColumns() {
        StringBuilder result = new StringBuilder();
        result.append("Result:\n");
    
        // Get ordered column list specified in the query
        List<String> selectedColumns = (List<String>) parsedSQL.get("target");
    
        // Print header for selected columns in the specified order
        for (String column : selectedColumns) {
            result.append(column).append("\t");
        }
        result.append("\n");
    
        // Print each row with values aligned according to selected column order
        for (int rowKey : table.getTable().keySet()) {
            Map<String, Object> row = table.getTable().get(rowKey);
            if (rowKey == 0) continue; // Skip header row if present
    
            // Apply WHERE clause filtering (if present)
            if (parsedSQL.containsKey("whereConditionColumn") && !evaluateWhereClause(row)) continue;
    
            // Print row values in the order of selected columns
            for (String column : selectedColumns) {
                Object value = row.getOrDefault(column, "NULL");
                result.append(value.toString()).append("\t");
            }
            result.append("\n");
        }
    
        return result.toString();
    }


    public String update(HashMap<String, Object> parsedSQL) {
        if (!database.containsKey(tableName)) {
            return "ERROR: Table " + tableName + " does not exist";
        }

        Table table = database.get(tableName);

        if (table == null) {
            return "ERROR: Table is not initialized correctly.";
        }

        HashMap<String, Object> updates = new HashMap<>();
        List<String> updateTargets = (List<String>) parsedSQL.get("target");
        for (int i = 0; i < updateTargets.size(); i += 2) {
            updates.put(updateTargets.get(i), updateTargets.get(i + 1));
        }

        int updatedRows = table.updateRow(updates,
            (String) parsedSQL.get("whereConditionColumn"),
            (String) parsedSQL.get("whereOperator"),
            parsedSQL.get("whereValue"),
            (String) parsedSQL.get("secondCondition"),
            (String) parsedSQL.get("secondConditionColumn"),
            (String) parsedSQL.get("secondOperator"),
            parsedSQL.get("secondValue")
        );

        return "Updated " + updatedRows + " row(s) in " + tableName;
    }

    public String create(HashMap<String, Object> parsedSQL) {
        
        //error
        if (database.containsKey(tableName)) {
            return "ERROR: Table already exists";
        }

        // Initialize an empty table (HashMap of rows where each row has a primary key)
        List<String> columnNames = (List<String>) parsedSQL.get("columns");

        //create a new hashmap table
        Table newTable = new Table(columnNames);

        database.put(tableName, newTable);

        return "Table " + tableName + " created successfully";
    }

    private boolean evaluateWhereClause(Map<String, Object> row) {
        String columnName = (String) parsedSQL.get("whereConditionColumn");
        String operator = (String) parsedSQL.get("whereOperator");
        Object expectedValue = parsedSQL.get("whereValue");

        boolean whereResult = evaluateCondition(row, columnName, operator, expectedValue);

        // Handle a second condition if it exists
        if (parsedSQL.containsKey("secondCondition")) {
            String secondCondition = (String) parsedSQL.get("secondCondition");
            columnName = (String) parsedSQL.get("secondConditionColumn");
            operator = (String) parsedSQL.get("secondOperator");
            expectedValue = parsedSQL.get("secondValue");

            boolean secondResult = evaluateCondition(row, columnName, operator, expectedValue);

            if (secondCondition.equalsIgnoreCase("AND")) {
                whereResult &= secondResult;
            } else if (secondCondition.equalsIgnoreCase("OR")) {
                whereResult |= secondResult;
            }
        }
        return whereResult;
    }

    private boolean evaluateCondition(Map<String, Object> row, String columnName, String operator, Object expectedValue) {
        Object columnValue = row.get(columnName);
        if (columnValue == null) return false;

        switch (operator) {
            case "=":
                return columnValue.equals(expectedValue);
            case "<":
                return Double.parseDouble(columnValue.toString()) < Double.parseDouble(expectedValue.toString());
            case ">":
                return Double.parseDouble(columnValue.toString()) > Double.parseDouble(expectedValue.toString());
            default:
                return false;
        }
    }

}
