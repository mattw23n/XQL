package edu.smu.smusql.HashMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.smu.smusql.CustomParser;
import edu.smu.smusql.Engine;
 
public class HashMapEngine extends Engine {

    
    private final Map<String,Table> database = new HashMap<>();

    @Override
    public String executeSQL(String query) {
        try {
            HashMap<String, Object> tokens = CustomParser.parseSQL(query);
            
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

        String tableName = (String) tokens.get("tableName");

        //error
        if (!database.containsKey(tableName)) {
            return "ERROR: Table does not exist";
        }

        List<String> columns = (List<String>) tokens.get("columns");

        List<Object> values = (List<Object>) tokens.get("values");

        // Check column count against values count
        if (columns.size() != values.size()) {
            return "ERROR: Column count doesn't match value count";
        }

        //get table & insert row
        Table table = database.get(tableName);
        String primaryKey = columns.get(0).trim();
        Object[] rowValues = values.toArray();

        // Insert the new row into the table
        table.addRow(tableName, primaryKey, rowValues);

        return "Row inserted into " + tableName + " with primary key " + primaryKey;
    }

    public String delete(HashMap<String, Object> tokens) {

        String tableName = (String) tokens.get("tableName");
        
        if (!database.containsKey(tableName)) {
            return "ERROR: Table does not exist";
        }

        // Retrieve the table and delete the row
        Table table = database.get(tableName);
        String primaryKey = (String) tokens.get("whereValue");

        table.deleteRow(tableName, primaryKey);

        return "Row with primary key " + primaryKey + " deleted from " + tableName;
    }

    public String select(HashMap<String, Object> tokens) {
        String tableName = (String) tokens.get("tableName");

        // Error if table does not exist
        if (!database.containsKey(tableName)) {
            return "ERROR: Table " + tableName + " does not exist";
        }

        // Retrieve the table and select all rows
        Table table = database.get(tableName);
        return table.selectAllRows(tableName);
    }

    public String update(HashMap<String, Object> tokens) {
        String tableName = (String) tokens.get("tableName");

        // Check if the table exists
        if (!database.containsKey(tableName)) {
            return "ERROR: Table " + tableName + " does not exist";
        }

        Table table = database.get(tableName);
        String primaryKey = (String) tokens.get("whereValue");
        Map<String, Object> updatedData = new HashMap<>();

        List<String> columnsToUpdate = (List<String>) tokens.get("target");
        for (int i = 0; i < columnsToUpdate.size(); i += 2) {
            updatedData.put(columnsToUpdate.get(i), columnsToUpdate.get(i + 1));
        }

        // Update the specified row
        table.updateRow(tableName, primaryKey, updatedData);

        return "Row with primary key " + primaryKey + " updated in " + tableName;
    }


    public String create(HashMap<String, Object> tokens) {
        
        String tableName = (String) tokens.get("tableName"); 

        //error
        if (database.containsKey(tableName)) {
            return "ERROR: Table already exists";
        }

        // Initialize an empty table (HashMap of rows where each row has a primary key)
        List<String> columnNames = (List<String>) tokens.get("columns");

        //create a new hashmap table
        //Table newTable = new Table(columnNames);

        database.put(tableName, new Table(columnNames));

        return "Table " + tableName + " created successfully";
    }
}
