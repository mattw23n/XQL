package edu.smu.smusql;

import java.util.*;

public class Schema {
    private List<Table> tables;

    public Schema(List<Table> tables) {
        this.tables = tables;
    }

    // Add a new table to the schema
    public void addTable(Table table) {
        tables.add(table);
        System.out.println("Table added");
    }

    // Get a table by its name
    public Table getTableByName(String tableName) {
        for (Table table : tables) {
            if (table.getTableName().equals(tableName)) {
                return table;
            }
        }
        throw new IllegalArgumentException("Table " + tableName + " does not exist in the database");
    }

    // Remove a table by its name
    public void removeTable(String tableName) {
        tables.removeIf(table -> table.getTableName().equals(tableName));
        System.out.println("Table removed");
    }

    // Print all tables in the schema
    public void printSchema() {
        System.out.println("Tables: ");
        for (Table table : tables) {
            System.out.println("- " + table.getTableName());
        }
    }
}
