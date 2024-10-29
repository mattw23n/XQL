package edu.smu.smusql.GPT;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.smu.smusql.CustomParser;
import edu.smu.smusql.Engine;

public class GPTEngine extends Engine{
    private static String name = "GPT";
    private static String[][] stats = {{"Untested"}, {"AI-generated Approach"}};

    public GPTEngine(){
        super(name, stats);
    }


    public String getName() {
        return name;
    }

    public String[][] getStats() {
        return stats;
    }
    
    

    // Stores tables in the database
    private HashMap<String, Table> tables = new HashMap<>();

    public GPTEngine(String name, String[][] stats2) {
        super(name, stats2);
        //TODO Auto-generated constructor stub
    }

    // Execute SQL based on parsed HashMap data from CustomParser
    public String executeSQL(String query) {
        HashMap<String, Object> map;
        
        try { 
            map = CustomParser.parseSQL(query); 
        } catch (Exception e) { 
            return "ERROR: " + e.getMessage(); 
        }

        String command = (String) map.get("command");

        switch (command) {
            case "CREATE":
                return create(map);
            case "INSERT":
                return insert(map);
            case "SELECT":
                return select(map);
            case "UPDATE":
                return update(map);
            case "DELETE":
                return delete(map);
            default:
                return "ERROR: Unknown command";
        }
    }

    // CREATE TABLE
    public String create(HashMap<String, Object> map) {
        String tableName = (String) map.get("tableName");
        List<String> columns = (List<String>) map.get("columns");
        
        if (tables.containsKey(tableName)) {
            return "ERROR: Table already exists";
        }

        // Create and store the table
        Table newTable = new Table(tableName, columns);
        tables.put(tableName, newTable);

        return "Table " + tableName + " created.";
    }

    // INSERT INTO TABLE
    public String insert(HashMap<String, Object> map) {
        String tableName = (String) map.get("tableName");
        List<String> values = (List<String>) map.get("columns");

        if (!tables.containsKey(tableName)) {
            return "ERROR: Table " + tableName + " does not exist";
        }

        Table table = tables.get(tableName);
        table.insert(values); // Insert values into the table
        
        return "Row inserted into " + tableName;
    }

    // SELECT *
    public String select(HashMap<String, Object> map) {
        String tableName = (String) map.get("tableName");
        Table table = tables.get(tableName);
        
        if (table == null) {
            return "ERROR: Table " + tableName + " not found";
        }

        // Handle WHERE clause conditions if any
        StringBuilder result = new StringBuilder();
        if (map.containsKey("whereConditionColumn")) {
            result.append(table.selectWhere(map));
        } else {
            result.append(table.selectAll());
        }
        return result.toString();
    }

    // UPDATE
    public String update(HashMap<String, Object> map) {
        String tableName = (String) map.get("tableName");
        Table table = tables.get(tableName);

        if (table == null) {
            return "ERROR: Table " + tableName + " not found";
        }

        int updatedRows = table.update(map);
        return updatedRows + " rows updated in " + tableName;
    }

    // DELETE
    public String delete(HashMap<String, Object> map) {
        String tableName = (String) map.get("tableName");
        Table table = tables.get(tableName);

        if (table == null) {
            return "ERROR: Table " + tableName + " not found";
        }

        int deletedRows = table.delete(map);
        return deletedRows + " rows deleted from " + tableName;
    }
}

