package edu.smu.smusql.CircularLinkedList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import edu.smu.smusql.CustomParser;
import edu.smu.smusql.Engine;

public class CLLEngine extends Engine {

    private static String name = "Circular Linked List";
    private static String[][] stats = { { "" }, { "" } };

    public CLLEngine() {
        super(name, stats);
    }

    public String executeSQL(String query) {
        HashMap<String, Object> parsedQuery = CustomParser.parseSQL(query);
        if (parsedQuery == null) {
            return "ERROR: Unknown command";
        }

        String command = (String) parsedQuery.get("command");

        switch (command) {
            case "CREATE":
                return create(parsedQuery);
            case "INSERT":
                return insert(parsedQuery);
            case "SELECT":
                return select(parsedQuery);
            case "UPDATE":
                return update(parsedQuery);
            case "DELETE":
                return delete(parsedQuery);
            default:
                return "ERROR: Unknown command";
        }
    }

    Database db = new Database();

    // CREATE TABLE table_name (column1, column2, ...)
    public String create(HashMap<String, Object> parsedQuery) {
        String tableName = (String) parsedQuery.get("tableName");
        List<String> columns = (List<String>) parsedQuery.get("columns");

        // Extract column names and types
        String[] columnNames = new String[columns.size()];
        String[] dataTypes = new String[columns.size()];

        for (int i = 0; i < columns.size(); i++) {
            String columnName = columns.get(i);
            // If data type is not provided, assume a default data type (e.g., VARCHAR)
            columnNames[i] = columnName;
            dataTypes[i] = "VARCHAR"; // You can change this to another default data type if needed
        }

        // Create a new table
        Table table = new Table(tableName, columnNames, dataTypes);

        // Add the table to the database
        db.addTable(table);

        return "Table " + tableName + " created successfully.";
    }

    // INSERT INTO table_name VALUES (value1, value2, ...)
    public String insert(HashMap<String, Object> parsedQuery) {
        String tableName = (String) parsedQuery.get("tableName");
        List<String> values = (List<String>) parsedQuery.get("columns");

        // Find the table by name (in practice, CircularlyLinkedList will store Table
        // objects)
        Table table = db.findTableByName(tableName);
        if (table == null)
            return "Table not found";

        // Ensure the number of values matches the number of columns
        if (values.size() != table.getColumnSize()) {
            return "ERROR: Number of values does not match the number of columns";
        }

        // Add values to table rows
        table.getRows().addLast(values.toArray(String[]::new));

        return "Row inserted into table " + tableName;
    }

    // SELECT * FROM table_name WHERE condition
    public String select(HashMap<String, Object> parsedQuery) {
        String tableName = (String) parsedQuery.get("tableName");
        Table table = db.findTableByName(tableName);
        if (table == null)
            return "Table not found";

        // Determine target columns
        Object targetObj = parsedQuery.get("target");
        List<String> targetColumns = new ArrayList<>();
        boolean selectAllColumns = false;

        if (targetObj instanceof String && targetObj.equals("*")) {
            selectAllColumns = true;
        } else if (targetObj instanceof List) {
            targetColumns = (List<String>) targetObj;
        } else {
            return "ERROR: Invalid target columns format";
        }

        // Condition parsing
        String conditionColumn = (String) parsedQuery.get("whereConditionColumn");
        String whereOperator = (String) parsedQuery.get("whereOperator");
        String conditionValue = (String) parsedQuery.get("whereValue");

        String secondCondition = (String) parsedQuery.get("secondCondition");
        String secondConditionColumn = (String) parsedQuery.get("secondConditionColumn");
        String secondOperator = (String) parsedQuery.get("secondOperator");
        String secondValue = (String) parsedQuery.get("secondValue");

        int conditionColumnIndex = -1;
        int secondConditionColumnIndex = -1;

        // Prepare results output
        StringBuilder result = new StringBuilder();
        result.append("Results from ").append(tableName).append(":\n");

        CircularlyLinkedList<String[]> rows = table.getRows();
        int initialSize = rows.size();
        for (int i = 0; i < initialSize; i++) {
            String[] row = rows.removeFirst();

            // Find column indices if conditions are specified
            if (conditionColumn != null && conditionColumnIndex == -1) {
                conditionColumnIndex = table.getColumnIndex(conditionColumn);
            }
            if (secondConditionColumn != null && secondConditionColumnIndex == -1) {
                secondConditionColumnIndex = table.getColumnIndex(secondConditionColumn);
            }

            // Evaluate conditions if specified
            boolean conditionMet = conditionColumn == null ||
                    (conditionColumnIndex != -1 && conditionColumnIndex < row.length &&
                            evaluateCondition(row[conditionColumnIndex], whereOperator, conditionValue));
            boolean secondConditionMet = secondConditionColumn == null ||
                    (secondConditionColumnIndex != -1 && secondConditionColumnIndex < row.length &&
                            evaluateCondition(row[secondConditionColumnIndex], secondOperator, secondValue));

            // Determine if the row should be selected
            boolean selectRow = (conditionColumn == null && secondConditionColumn == null) // No conditions specified
                    || (secondCondition == null ? conditionMet
                            : secondCondition.equalsIgnoreCase("AND") ? conditionMet && secondConditionMet
                                    : conditionMet || secondConditionMet);

            // Collect selected columns or all columns if conditions are met
            if (selectRow) {
                if (selectAllColumns) {
                    result.append(Arrays.toString(row)).append("\n");
                } else {
                    StringBuilder rowResult = new StringBuilder("[");
                    for (String column : targetColumns) {
                        int colIndex = table.getColumnIndex(column);
                        if (colIndex != -1)
                            rowResult.append(row[colIndex]).append(", ");
                    }
                    rowResult.setLength(rowResult.length() - 2); // Remove last comma and space
                    rowResult.append("]");
                    result.append(rowResult.toString()).append("\n");
                }
            }

            rows.addLast(row);
        }

        return result.toString();
    }

    // DELETE FROM table_name WHERE condition (delete by index or condition)
    public String delete(HashMap<String, Object> parsedQuery) {
        String tableName = (String) parsedQuery.get("tableName");
        String conditionColumn = (String) parsedQuery.get("whereConditionColumn");
        String whereOperator = (String) parsedQuery.get("whereOperator");
        String conditionValue = (String) parsedQuery.get("whereValue");
        String secondCondition = (String) parsedQuery.get("secondCondition");
        String secondConditionColumn = (String) parsedQuery.get("secondConditionColumn");
        String secondOperator = (String) parsedQuery.get("secondOperator");
        String secondValue = (String) parsedQuery.get("secondValue");

        int conditionColumnIndex = -1;
        int secondConditionColumnIndex = -1;

        Table table = db.findTableByName(tableName);
        if (table == null)
            return "Table not found";

        CircularlyLinkedList<String[]> rows = table.getRows();
        int initialSize = rows.size();
        for (int i = 0; i < initialSize; i++) {
            String[] row = rows.removeFirst();

            // Find the index of the condition column
            if (conditionColumnIndex == -1) {
                for (int j = 0; j < table.getColumnSize(); j++) {
                    String[] column = table.getColumns().get(j);
                    if (column[0].equals(conditionColumn)) {
                        conditionColumnIndex = j;
                        break;
                    }
                }
            }

            // Find the index of the second condition column
            if (secondConditionColumnIndex == -1 && secondConditionColumn != null) {
                for (int j = 0; j < table.getColumnSize(); j++) {
                    String[] column = table.getColumns().get(j);
                    if (column[0].equals(secondConditionColumn)) {
                        secondConditionColumnIndex = j;
                        break;
                    }
                }
            }

            // Ensure indices are within bounds before accessing
            if (conditionColumnIndex >= 0 && secondConditionColumnIndex >= 0) {
                // Evaluate the condition
                boolean conditionMet = evaluateCondition(row[conditionColumnIndex], whereOperator, conditionValue);
                boolean secondConditionMet = secondCondition != null
                        ? evaluateCondition(row[secondConditionColumnIndex], secondOperator, secondValue)
                        : false;

                // Combine conditions based on the operator
                boolean deleteRow = (secondCondition == null || secondCondition.equalsIgnoreCase("AND"))
                        ? conditionMet && secondConditionMet
                        : conditionMet || secondConditionMet;

                // If the row should be deleted, do not add it back to the list
                if (!deleteRow) {
                    rows.addLast(row); // Only add the row back if it should NOT be deleted
                }
            }
        }

        return "Rows deleted from table " + tableName;
    }

    // UPDATE table_name SET column=value WHERE condition (update by index for now)
    public String update(HashMap<String, Object> parsedQuery) {
        String tableName = (String) parsedQuery.get("tableName");
        List<String> columnsToUpdate = (List<String>) parsedQuery.get("target");
        String conditionColumn = (String) parsedQuery.get("whereConditionColumn");
        String whereOperator = (String) parsedQuery.get("whereOperator");
        String conditionValue = (String) parsedQuery.get("whereValue");
        String secondCondition = (String) parsedQuery.get("secondCondition");
        String secondConditionColumn = (String) parsedQuery.get("secondConditionColumn");
        String secondOperator = (String) parsedQuery.get("secondOperator");
        String secondValue = (String) parsedQuery.get("secondValue");

        int conditionColumnIndex = -1;
        int secondConditionColumnIndex = -1;

        // Find the table
        Table table = db.findTableByName(tableName);
        if (table == null)
            return "Table not found";

        CircularlyLinkedList<String[]> rows = table.getRows();
        int initialSize = rows.size();

        // Iterate over the rows
        for (int i = 0; i < initialSize; i++) {
            String[] row = rows.removeFirst(); // Always remove the first row for processing

            // Find the index of the condition column if not already found
            if (conditionColumnIndex == -1) {
                for (int j = 0; j < table.getColumnSize(); j++) {
                    String[] column = table.getColumns().get(j);
                    if (column[0].equals(conditionColumn)) {
                        conditionColumnIndex = j;
                        break;
                    }
                }
            }

            // Find the index of the second condition column if applicable
            if (secondConditionColumnIndex == -1 && secondConditionColumn != null) {
                for (int j = 0; j < table.getColumnSize(); j++) {
                    String[] column = table.getColumns().get(j);
                    if (column[0].equals(secondConditionColumn)) {
                        secondConditionColumnIndex = j;
                        break;
                    }
                }
            }

            // Evaluate the first condition
            boolean conditionMet = evaluateCondition(row[conditionColumnIndex], whereOperator, conditionValue);

            // Evaluate the second condition if provided
            boolean secondConditionMet = true; // Default to true if there is no second condition
            if (secondCondition != null) {
                secondConditionMet = evaluateCondition(row[secondConditionColumnIndex], secondOperator, secondValue);
            }

            // Determine whether the row should be updated based on the conditions
            boolean updateRow = secondCondition == null ? conditionMet
                    : secondCondition.equalsIgnoreCase("AND") ? conditionMet && secondConditionMet
                            : conditionMet || secondConditionMet;

            // If the row meets the condition, update the specified columns
            if (updateRow) {
                for (int j = 0; j < columnsToUpdate.size(); j += 2) {
                    String columnName = columnsToUpdate.get(j);
                    String newValue = columnsToUpdate.get(j + 1);
                    int updateColumnIndex = -1;

                    // Find the index of the column to update
                    for (int k = 0; k < table.getColumnSize(); k++) {
                        String[] column = table.getColumns().get(k);
                        if (column[0].equals(columnName)) {
                            updateColumnIndex = k;
                            break;
                        }
                    }

                    // Update the value in the row if the column was found
                    if (updateColumnIndex != -1) {
                        row[updateColumnIndex] = newValue;
                    }
                }
            }

            // Add the row back to the list after processing
            rows.addLast(row);
        }

        return "Table " + tableName + " updated.";
    }

    private boolean evaluateCondition(String value, String operator, String conditionValue) {
        switch (operator) {
            case "=":
                return value.equals(conditionValue);
            case "<":
                return Double.parseDouble(value) < Double.parseDouble(conditionValue);
            case ">":
                return Double.parseDouble(value) > Double.parseDouble(conditionValue);
            case "<=":
                return Double.parseDouble(value) <= Double.parseDouble(conditionValue);
            case ">=":
                return Double.parseDouble(value) >= Double.parseDouble(conditionValue);
            default:
                return false;
        }
    }
}
