package edu.smu.smusql.HashMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Table {

    private HashMap<Integer, HashMap<String, Object>> table;
    private List<String> columnOrder;

    public Table(List<String> columnNames) {
        this.columnOrder = columnNames;
        this.table = new HashMap<>();
        this.table.put(0, createEmptyRow());
    }

    public List<String> getColumnOrder() {
        return columnOrder;
    }

    public HashMap<Integer, HashMap<String, Object>> getTable() {
        return table;
    }

    public HashMap<String, Object> getRow(int rowId) {
        return table.get(rowId);
    }

    public void addRow(HashMap<String, Object> newRow) {
        // if (!table.containsKey(tableName)) {
        //     return "ERROR: Table " + tableName + " does not exist";
        // }

        int rowId = table.size();
        table.put(rowId, newRow);
    }

    private HashMap<String, Object> createEmptyRow() {
        HashMap<String, Object> emptyRow = new HashMap<>();

        for (String columnName : columnOrder) {
            emptyRow.put(columnName, null);
        }
        return emptyRow;
    }

    public void deleteRow(int rowId) {
        
        table.remove(rowId);

        // return "Row with primary key " + primaryKey + " deleted from " + tableName;
    }

    public void updateRow(int rowId, HashMap<String, Object> updatedValues) {
        
        HashMap<String, Object> row = table.get(rowId);

        if (row != null) {
            for (Map.Entry<String, Object> entry : updatedValues.entrySet()) {
                row.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public void printTable() {
        System.out.println("Columns: " + columnOrder);
        for (Map.Entry<Integer, HashMap<String, Object>> rowEntry : table.entrySet()) {
            System.out.println("Row ID " + rowEntry.getKey() + ": " + rowEntry.getValue());
        }
    }

    public HashMap<Integer, Object> getColumnData(String columnName) {
        HashMap<Integer, Object> columnData = new HashMap<>();
        for (Map.Entry<Integer, HashMap<String, Object>> rowEntry : table.entrySet()) {
            columnData.put(rowEntry.getKey(), rowEntry.getValue().get(columnName));
        }
        return columnData;
    }

    // //helper method
    // private String formatRow(Object[] row) {
    //     StringBuilder rowString = new StringBuilder("[");
    //     for (int i = 0; i < row.length; i++) {
    //         rowString.append(columnOrder.get(i)).append(": ").append(row[i]);
    //         if (i < row.length - 1) rowString.append(", ");
    //     }
    //     rowString.append("]");
    //     return rowString.toString();
    // }

}