package edu.smu.smusql.Treemap;

import java.util.*;

/*
 * Tables represented in treemap
 * level 1: Treemap representing tablename (String): Each Row / entry (Tree Map)
 * level 2: The row's treemap representing each row (int): each column (Tree Map)
 * level 3: The column's treemap represnting each column name (String): respective value (Object)
 */
public class Table {
    private TreeMap<Integer, TreeMap<String, Object>> table;
    private List<String> originalColunmnOrder;

    public Table(List<String> columnsNames) {
        this.originalColunmnOrder = columnsNames;
        TreeMap<String, Object> columns = new TreeMap<>();
        for (String string : columnsNames) {
            columns.put(string, null);
        }

        // initialize first row
        this.table = new TreeMap<>();
        this.table.put(0, columns);
    }

    public TreeMap<Integer, TreeMap<String, Object>> getTable() {
        return table;
    }
    
    public List<String> getColumnOrder(){
        return originalColunmnOrder;
    }

    

    public void addRow(TreeMap<String, Object> new_row) {
        int newEntryInt = this.table.lastKey() + 1;

        table.put(newEntryInt, new_row);
    }

}
