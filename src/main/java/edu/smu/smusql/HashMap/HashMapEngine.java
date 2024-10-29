package edu.smu.smusql.HashMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.smu.smusql.CustomParser;
import edu.smu.smusql.Engine;
 
public class HashMapEngine extends Engine {

    private final Map<String,Table> database = new HashMap<>();
    
    HashMap<String, Object> tokens;
    String tableName;
    Table table;

    private static String name = "HashMap";
    private static String[][] stats = {{"Time"}, {"Test"}};
    
    public HashMapEngine() {
        super(name, stats);
    }

    public String executeSQL(String query) {
        try {
            tokens = CustomParser.parseSQL(query);
//System.out.println("Parsed Tokens: " + tokens); 

            tableName = (String) tokens.get("tableName");

            // Error handling for parsing
            if (tokens == null || !tokens.containsKey("command")) {
                return "ERROR: Unable to read command";
            }

            String command = (String) tokens.get("command");

            // Handle commands based on their type
            return switch (command.toUpperCase()) {
                case "CREATE" -> create(tokens);
                case "INSERT" -> insert(tokens);
                case "SELECT" -> select(tokens);
                case "UPDATE" -> update(tokens);
                case "DELETE" -> delete(tokens);
                default -> "ERROR: Unknown command";
            };
        } catch (Exception e) {
            return "ERROR: " + e.getMessage();
        }
    }

    public String insert(HashMap<String, Object> tokens) {

        //error
        if (!database.containsKey(tableName)) {
            return "ERROR: Table does not exist";
        }

        Table table = database.get(tableName);

        List<String> columns = (List<String>) tokens.get("columns");

        List<Object> values = (List<Object>) tokens.get("values");

            // If values are null, use columns for both names and values
    if (values == null) {
        values = (List<Object>) tokens.get("columns");
        columns = table.getColumnOrder();  // Get column order from table definition
    }

        // Create a row with specified columns and values
        HashMap<String, Object> newRow = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            newRow.put(columns.get(i), values.get(i));
        }

        // Add the new row to the table
        table.addRow(newRow);

        return "Row inserted into " + tableName;
    }

    public String delete(HashMap<String, Object> tokens) {
        // Error if table does not exist
        if (!database.containsKey(tableName)) {
            return "ERROR: Table does not exist";
        }

        //Table table = database.get(tableName);

        // Retrieve the primary key for row deletion
        String primaryKey = (String) tokens.get("whereValue");

        return "ERROR: Row with primary key " + primaryKey + " not found in " + tableName;   
    }

    public String select(HashMap<String, Object> tokens) {

        // Error if table does not exist
        if (!database.containsKey(tableName)) {
            return "ERROR: Table " + tableName + " does not exist";
        }

        // Retrieve the table and select all rows
        Table table = database.get(tableName);
        if (table == null) {
            return "ERROR: Table is not initialized correctly.";
        }
        // Assuming we're selecting all rows and columns
        StringBuilder result = new StringBuilder();
        result.append("Table ").append(tableName).append(":\n");

        // Add column headers
        result.append(String.join(", ", table.getColumnOrder())).append("\n");

        // Add row data
        for (Map.Entry<Integer, HashMap<String, Object>> row : table.getTable().entrySet()) {
            for (String column : table.getColumnOrder()) {
                result.append(row.getValue().getOrDefault(column, "NULL")).append("\t");
            }
            result.append("\n");
        }

        return result.toString();
    }

    public String update(HashMap<String, Object> tokens) {

        // Check if the table exists
        if (!database.containsKey(tableName)) {
            return "ERROR: Table " + tableName + " does not exist";
        }

        Table table = database.get(tableName);

        String primaryKey = (String) tokens.get("whereValue");

        // Parse columns and values to update
        List<String> columnsToUpdate = (List<String>) tokens.get("target");
        
        HashMap<String, Object> updatedData = new HashMap<>();
        for (int i = 0; i < columnsToUpdate.size(); i += 2) {
            updatedData.put(columnsToUpdate.get(i), columnsToUpdate.get(i + 1));
        }
    
        return "Row with primary key " + primaryKey + " updated in " + tableName;

    }


    public String create(HashMap<String, Object> tokens) {
        
        //error
        if (database.containsKey(tableName)) {
            return "ERROR: Table already exists";
        }

        // Initialize an empty table (HashMap of rows where each row has a primary key)
        List<String> columnNames = (List<String>) tokens.get("columns");

        //create a new hashmap table
        Table newTable = new Table(columnNames);

        database.put(tableName, newTable);

        return "Table " + tableName + " created successfully";
    }
}
