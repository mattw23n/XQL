package edu.smu.smusql.HashMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Table {

    private final Map<String, Map<String, Object[]>> tableDB;
    //private final Map<String, Object[]> table;

    private final List<String> columnOrder;

    public Table(List<String> columnNames) {
        this.columnOrder = columnNames;
        this.tableDB = new HashMap<>();
    }

    public List<String> getColumnOrder() {
        return columnOrder;
    }

    public Map<String, Map<String, Object[]>> getTable() {
        return tableDB;
    }

    public String addRow(String tableName, String primaryKey, Object[] rowData) {
        if (!tableDB.containsKey(tableName)) {
            return "ERROR: Table " + tableName + " does not exist";
        }

        Map<String, Object[]> table = tableDB.get(tableName);
        if (table.containsKey(primaryKey)) {
            return "ERROR: Row with primary key " + primaryKey + " already exists in " + tableName;
        }

        // Ensure rowData has the same length as columns, padding if necessary
        Object[] fullRowData = new Object[columnOrder.size()];

        System.arraycopy(rowData, 0, fullRowData, 0, Math.min(rowData.length, columnOrder.size()));

        table.put(primaryKey, fullRowData);
        
        return "Row added to " + tableName + " with primary key " + primaryKey;
    }


    public String deleteRow(String tableName, String primaryKey) {
        if (!tableDB.containsKey(tableName)) {
            return "ERROR: Table " + tableName + " does not exist";
        }

        Map<String, Object[]> table = tableDB.get(tableName);
        if (!table.containsKey(primaryKey)) {
            return "ERROR: No row with primary key " + primaryKey + " found in " + tableName;
        }

        table.remove(primaryKey);

        return "Row with primary key " + primaryKey + " deleted from " + tableName;
    }

    public String updateRow(String tableName, String primaryKey, Map<String, Object> updatedData) {
        
        if (!tableDB.containsKey(tableName)) {
            return "ERROR: Table " + tableName + " does not exist";
        }

        Map<String, Object[]> table = tableDB.get(tableName);
        if (!table.containsKey(primaryKey)) {
            return "ERROR: No row with primary key " + primaryKey + " found in " + tableName;
        }

        Object[] row = table.get(primaryKey);
        for (Map.Entry<String, Object> entry : updatedData.entrySet()) {
            int columnIndex = columnOrder.indexOf(entry.getKey());
            if (columnIndex >= 0) {
                row[columnIndex] = entry.getValue();
            }
        }
        return "Row with primary key " + primaryKey + " updated in " + tableName;
    }

    public String selectAllRows(String tableName) {
        if (!tableDB.containsKey(tableName)) {
            return "ERROR: Table " + tableName + " does not exist";
        }

        Map<String, Object[]> table = tableDB.get(tableName);
        if (table.isEmpty()) {
            return "Table " + tableName + " is empty";
        }

        StringBuilder result = new StringBuilder();
        for (Map.Entry<String, Object[]> entry : table.entrySet()) {
            result.append("Key: ").append(entry.getKey())
                  .append(", Values: ").append(formatRow(entry.getValue()))
                  .append("\n");
        }

        return result.toString();
    }

    //helper method
    private String formatRow(Object[] row) {
        StringBuilder rowString = new StringBuilder("[");
        for (int i = 0; i < row.length; i++) {
            rowString.append(columnOrder.get(i)).append(": ").append(row[i]);
            if (i < row.length - 1) rowString.append(", ");
        }
        rowString.append("]");
        return rowString.toString();
    }

}