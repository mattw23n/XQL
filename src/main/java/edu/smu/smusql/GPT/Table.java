package edu.smu.smusql.GPT;

import java.util.*;

public class Table {
    
    private String tableName;
    private List<String> columns;
    private List<Map<String, String>> rows = new ArrayList<>();

    public Table(String tableName, List<String> columns) {
        this.tableName = tableName;
        this.columns = columns;
    }

    // Insert a row into the table
    public void insert(List<String> values) {
        Map<String, String> row = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            row.put(columns.get(i), values.get(i));
        }
        rows.add(row);
    }

    // SELECT * with no condition
    public String selectAll() {
        StringBuilder result = new StringBuilder();
        result.append(String.join("\t", columns)).append("\n");
        
        for (Map<String, String> row : rows) {
            for (String column : columns) {
                result.append(row.getOrDefault(column, "NULL")).append("\t");
            }
            result.append("\n");
        }
        return result.toString();
    }

    // SELECT * WHERE condition
    public String selectWhere(HashMap<String, Object> map) {
        StringBuilder result = new StringBuilder();
        result.append(String.join("\t", columns)).append("\n");

        String conditionColumn = (String) map.get("whereConditionColumn");
        String conditionValue = (String) map.get("whereValue");

        for (Map<String, String> row : rows) {
            if (row.get(conditionColumn).equals(conditionValue)) {
                for (String column : columns) {
                    result.append(row.getOrDefault(column, "NULL")).append("\t");
                }
                result.append("\n");
            }
        }
        return result.toString();
    }

    // UPDATE with condition
    public int update(HashMap<String, Object> map) {
        String conditionColumn = (String) map.get("whereConditionColumn");
        String conditionValue = (String) map.get("whereValue");
        List<String> updates = (List<String>) map.get("target");

        int updatedRows = 0;

        for (Map<String, String> row : rows) {
            if (row.get(conditionColumn).equals(conditionValue)) {
                for (int i = 0; i < updates.size(); i += 2) {
                    String columnToUpdate = updates.get(i);
                    String newValue = updates.get(i + 1);
                    row.put(columnToUpdate, newValue);
                }
                updatedRows++;
            }
        }
        return updatedRows;
    }

    // DELETE with condition
    public int delete(HashMap<String, Object> map) {
        String conditionColumn = (String) map.get("whereConditionColumn");
        String conditionValue = (String) map.get("whereValue");

        Iterator<Map<String, String>> iterator = rows.iterator();
        int deletedRows = 0;

        while (iterator.hasNext()) {
            Map<String, String> row = iterator.next();
            if (row.get(conditionColumn).equals(conditionValue)) {
                iterator.remove();
                deletedRows++;
            }
        }
        return deletedRows;
    }
}
