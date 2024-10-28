package edu.smu.smusql;

import java.util.*;
// import java.util.concurrent.locks.Condition;

public class Table {
    private String tableName;
    private List<Row> rows;
    private List<String> columnNames;

    public Table(String tableName, List<String> columnNames) {
        this.tableName = tableName;
        this.rows = new ArrayList<>();
        this.columnNames = columnNames;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public int getColumnIndex(String columnName) {
        for (int i = 0; i < columnNames.size(); i++) {
            if (columnNames.get(i).equals(columnName)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Inputted column " + columnName + " does not exist.");
    }

    // Insert a new row
    public void insertRow(List<Object> values) {
        if (values.size() != columnNames.size()) {
            throw new IllegalArgumentException("Column count does not match");
        }
        rows.add(new Row(values));
        // System.out.println("INSERT successful");
    }

    // Select * all rows
    public String selectAllRows() {

        StringBuilder tableString = new StringBuilder();

        // Calculate the maximum width for each column, with additional padding
        int[] columnWidths = calculateColumnWidths(columnNames, rows, 2);

        // Append table headers with extra spacing
        for (int i = 0; i < columnNames.size(); i++) {
            tableString.append(String.format("%-" + columnWidths[i] + "s", columnNames.get(i)));
        }
        tableString.append("\n");

        // Append each row's values with extra spacing
        for (Row row : rows) {
            List<Object> rowValues = row.getData();
            for (int i = 0; i < rowValues.size(); i++) {
                tableString.append(String.format("%-" + columnWidths[i] + "s", rowValues.get(i).toString()));
            }
            tableString.append("\n");
        }

        return tableString.toString();
    }

    // Select * with condition
    public String selectRows(Condition condition) {

        StringBuilder result = new StringBuilder();

        // Calculate the maximum width for each column, with additional padding
        int[] columnWidths = calculateColumnWidths(columnNames, rows, 2);

        // Append table headers with extra spacing
        for (int i = 0; i < columnNames.size(); i++) {
            result.append(String.format("%-" + columnWidths[i] + "s", columnNames.get(i)));
        }
        result.append("\n");

        // Append each row's values that meet the condition, with extra spacing
        for (Row row : rows) {
            if (condition.test(row)) {
                List<Object> rowValues = row.getData();
                for (int i = 0; i < rowValues.size(); i++) {
                    result.append(String.format("%-" + columnWidths[i] + "s", rowValues.get(i).toString()));
                }
                result.append("\n");
            }
        }

        return result.toString();
    }

    // Select specific columns
    public String selectColumns(List<String> columnNamesToSelect) {

        StringBuilder result = new StringBuilder();

        // Get indexes of the columns to select and calculate column widths with padding
        List<Integer> columnIndexes = new ArrayList<>();
        for (String columnName : columnNamesToSelect) {
            columnIndexes.add(getColumnIndex(columnName));
        }

        int[] columnWidths = calculateColumnWidths(columnNamesToSelect, rows, columnIndexes, 2);

        // Append selected column headers with extra spacing
        for (int i = 0; i < columnNamesToSelect.size(); i++) {
            result.append(String.format("%-" + columnWidths[i] + "s", columnNamesToSelect.get(i)));
        }
        result.append("\n");

        // Append each row's selected values with extra spacing
        for (Row row : rows) {
            for (int i = 0; i < columnIndexes.size(); i++) {
                Object value = row.getColumn(columnIndexes.get(i));
                result.append(String.format("%-" + columnWidths[i] + "s", value.toString()));
            }
            result.append("\n");
        }

        return result.toString();
    }

    // Select specific columns with condition
    public String selectColumnsWithCondition(List<String> columnNamesToSelect, Condition condition) {
        
        StringBuilder result = new StringBuilder();

        // Get indexes of the columns to select and calculate column widths with padding
        List<Integer> columnIndexes = new ArrayList<>();
        for (String columnName : columnNamesToSelect) {
            columnIndexes.add(getColumnIndex(columnName));
        }
        int[] columnWidths = calculateColumnWidths(columnNamesToSelect, rows, columnIndexes, condition, 2);

        // Append selected column headers with extra spacing
        for (int i = 0; i < columnNamesToSelect.size(); i++) {
            result.append(String.format("%-" + columnWidths[i] + "s", columnNamesToSelect.get(i)));
        }
        result.append("\n");

        // Append each row's selected values (if the condition is met) with extra spacing
        for (Row row : rows) {
            if (condition.test(row)) {
                for (int i = 0; i < columnIndexes.size(); i++) {
                    Object value = row.getColumn(columnIndexes.get(i));
                    result.append(String.format("%-" + columnWidths[i] + "s", value.toString()));
                }
                result.append("\n");
            }
        }

        return result.toString();
    }

    // // Update rows based on a complex condition (original)
    // public void updateRows(Condition condition, Map<String, Object> newValues) {
    //     for (Row row : rows) {
    //         if (condition.test(row)) {
    //             // Update each column specified in the newValues map
    //             for (Map.Entry<String, Object> entry : newValues.entrySet()) {
    //                 String columnName = entry.getKey();
    //                 Object newValue = entry.getValue();
    //                 int columnIndex = columnNames.indexOf(columnName);
    
    //                 if (columnIndex != -1) {
    //                     row.updateColumn(columnIndex, newValue);
    //                 } else {
    //                     throw new IllegalArgumentException("Column " + columnName + " does not exist.");
    //                 }
    //             }
    //         }
    //     }
    // }

    // Update multiple column values based on condition (new)
    public void updateRows(Condition condition, Map<String, Object> newValues) {
        for (Row row : rows) {
            if (condition == null || condition.test(row)) {
                // Update each column specified in the newValues map
                for (Map.Entry<String, Object> entry : newValues.entrySet()) {
                    String columnName = entry.getKey();
                    Object newValue = entry.getValue();
                    int columnIndex = columnNames.indexOf(columnName);

                    if (columnIndex != -1) {
                        row.updateColumn(columnIndex, newValue);
                    } else {
                        throw new IllegalArgumentException("Column " + columnName + " does not exist.");
                    }
                }
            }
        }
        // System.out.println("UPDATE successful");
    }

    // Update multiple column values for all rows
    public void updateAllRows(Map<String, Object> newValues) {
        for (Row row : rows) {
            // Update each column specified in the newValues map
            for (Map.Entry<String, Object> entry : newValues.entrySet()) {
                String columnName = entry.getKey();
                Object newValue = entry.getValue();
                int columnIndex = columnNames.indexOf(columnName);

                if (columnIndex != -1) {
                    row.updateColumn(columnIndex, newValue);
                } else {
                    throw new IllegalArgumentException("Column " + columnName + " does not exist.");
                }
            }
        }
        // System.out.println("UPDATE successful");
    }

    // Delete rows with condition
    public void deleteRows(Condition condition) {
        rows.removeIf(condition::test);
        // System.out.println("DELETE successful");
    }

    // Delete all rows
    public void deleteAllRows() {
        rows.clear();  // Clears the entire list of rows
        // System.out.println("DELETE successful");
    }

    // Helper to calculate column widths based on data in each column with extra padding
    private int[] calculateColumnWidths(List<String> columnNames, List<Row> rows, int padding) {
        int[] widths = new int[columnNames.size()];

        // Start with the column header widths plus padding
        for (int i = 0; i < columnNames.size(); i++) {
            widths[i] = columnNames.get(i).length() + padding;
        }

        // Update the width based on row data plus padding
        for (Row row : rows) {
            List<Object> rowData = row.getData();
            for (int i = 0; i < rowData.size(); i++) {
                widths[i] = Math.max(widths[i], rowData.get(i).toString().length() + padding);
            }
        }

        return widths;
    }

    // Overloaded helper to calculate column widths for selected columns with padding, without conditions
    private int[] calculateColumnWidths(List<String> columnNamesToSelect, List<Row> rows, List<Integer> columnIndexes, int padding) {
        int[] widths = new int[columnIndexes.size()];

        // Start with the selected column header widths plus padding
        for (int i = 0; i < columnIndexes.size(); i++) {
            widths[i] = columnNamesToSelect.get(i).length() + padding;
        }

        // Update the width based on row data plus padding
        for (Row row : rows) {
            for (int i = 0; i < columnIndexes.size(); i++) {
                Object value = row.getColumn(columnIndexes.get(i));
                widths[i] = Math.max(widths[i], value.toString().length() + padding);
            }
        }

        return widths;
    }

    // Overloaded helper to calculate column widths for selected columns with conditionally selected rows and padding
    private int[] calculateColumnWidths(List<String> columnNamesToSelect, List<Row> rows, List<Integer> columnIndexes, Condition condition, int padding) {
        int[] widths = new int[columnIndexes.size()];

        // Start with the selected column header widths plus padding
        for (int i = 0; i < columnIndexes.size(); i++) {
            widths[i] = columnNamesToSelect.get(i).length() + padding;
        }

        // Update the width based on row data plus padding
        for (Row row : rows) {
            if (condition == null || condition.test(row)) {
                for (int i = 0; i < columnIndexes.size(); i++) {
                    Object value = row.getColumn(columnIndexes.get(i));
                    widths[i] = Math.max(widths[i], value.toString().length() + padding);
                }
            }
        }

        return widths;
    }

    
    @Override
    public String toString() {
        return "Table [tableName=" + tableName + ", rows=" + rows + ", columnNames=" + columnNames + "]";
    }
}
