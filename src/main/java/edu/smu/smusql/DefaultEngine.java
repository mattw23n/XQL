package edu.smu.smusql;

import java.util.HashMap;

public class DefaultEngine extends Engine {
    
    private static String name = "Default";
    private static String[][] stats = {{"completes in 30 mins", "mid"}};

    public DefaultEngine() {
        super(name, stats);

    }

    public String getName() {
        return name;
    }

    public String[][] getStats() {
        return stats;
    }

    public String executeSQL(String query) {
        HashMap<String, Object> map;
        
        try { 
            map = CustomParser.parseSQL(query); 
 
        } catch (Exception e) { 
            return e.getMessage(); 
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

    public String insert(HashMap<String, Object> map) {
        //TODO
        return "not implemented";
    }
    public String delete(HashMap<String, Object> map) {
        //TODO
        return "not implemented";
    }

    public String select(HashMap<String, Object> map) {
        //TODO
        return "not implemented";
    }
    public String update(HashMap<String, Object> map) {
        //TODO
        return "not implemented";
    }
    public String create(HashMap<String, Object> maps) {
        //TODO
        return "not implemented";
    }
}
