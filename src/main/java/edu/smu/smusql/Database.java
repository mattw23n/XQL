package edu.smu.smusql;

public class Database {
    private CircularlyLinkedList<Table> tables = new CircularlyLinkedList<>();

    public void addTable(Table table) {
        tables.addLast(table);
    }

    public Table findTableByName(String tableName) {
        for (int i = 0; i < tables.size(); i++) {
            Table table = tables.get(i);
            if (table.getTableName().equals(tableName)) {
                return table;
            }
        }
        return null;
    }
}