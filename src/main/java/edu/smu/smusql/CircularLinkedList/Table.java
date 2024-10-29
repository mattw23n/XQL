package edu.smu.smusql.CircularLinkedList;

/*
 * A hilariously bad implementation of a database table for smuSQL.
 * author: ziyuanliu@smu.edu.sg
 */
public class Table {
    private String tableName;
    private CircularlyLinkedList<String[]> columns = new CircularlyLinkedList<>(); // columnName and dataType
    private CircularlyLinkedList<String[]> rows = new CircularlyLinkedList<>(); // rows will store actual data

    public Table(String tableName, String[] columnNames, String[] dataTypes) {
        this.tableName = tableName;
        for (int i = 0; i < columnNames.length; i++) {
            columns.addLast(new String[] { columnNames[i], dataTypes[i] }); // Add column names and types
        }
    }

    public String getTableName() {
        return tableName;
    }

    public CircularlyLinkedList<String[]> getColumns() {
        return columns;
    }

    public CircularlyLinkedList<String[]> getRows() {
        return rows;
    }

    public int getColumnSize() {
        return columns.size();
    }

    public int getRowSize() {
        return rows.size();
    }

    public int getColumnIndex(String columnName) {
        for (int i = 0; i < columns.size(); i++) {
            String[] column = columns.get(i); // Retrieve each column array
            if (column[0].equalsIgnoreCase(columnName)) { // Check if name matches
                return i; // Return index if found
            }
        }
        return -1; // Return -1 if column name not found
    }
}
