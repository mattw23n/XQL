package edu.smu.smusql.MapHeapMap;

import java.util.*;

public class HeapTable {
    private MinHeap<Integer, Map<String, Object>> table;
    private List<String> originalColumnOrder;
    private int lastKey;

    public HeapTable(List<String> columnNames) {
        this.originalColumnOrder = columnNames;
        this.lastKey = 0;
        Map<String, Object> columns = new HashMap<>();

        for (String string : originalColumnOrder) {
            columns.put(string, null);
        }

        // initialize first row
        this.table = new MinHeap<>();
        this.table.insert(0, columns);
    }

    private void setLastKey(int newKey) {
        lastKey = newKey;
    }

    public MinHeap<Integer, Map<String, Object>> getTable() {
        return table;
    }

    public List<String> getColumnOrder() {
        return originalColumnOrder;
    }

    // Adds a new row to the table, using the next available integer as the row key
    public void addRow(Map<String, Object> newRow) {
        int newRowKey = lastKey + 1; // Use the next integer as the row key
        table.insert(newRowKey, newRow);  // Insert into MinHeap
        setLastKey(newRowKey);
    }
}

