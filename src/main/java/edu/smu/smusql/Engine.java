package edu.smu.smusql;

import java.util.HashMap;

public abstract class Engine {

    protected String name;
    
    public String getName() {
        return name;
    }

    public String[][] getStats() {
        return stats;
    }

    protected String[][] stats;

    // Constructor for initializing common attributes
    public Engine(String name, String[][] stats2) {
        this.name = name;
        this.stats = stats2;
    }

    public abstract String executeSQL(String query);

    public abstract String insert(HashMap<String, Object> map);

    public abstract String delete(HashMap<String, Object> map);

    public abstract String select(HashMap<String, Object> map);

    public abstract String update(HashMap<String, Object> map);

    public abstract String create(HashMap<String, Object> map);

}
