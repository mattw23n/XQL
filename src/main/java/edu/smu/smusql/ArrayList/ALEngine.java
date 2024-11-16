package edu.smu.smusql.ArrayList;

import java.util.*;

import edu.smu.smusql.CustomParser;
import edu.smu.smusql.Engine;

public class ALEngine extends Engine {

    private Schema schema = new Schema(new ArrayList<>());

    private static String name = "ArrayList";
    private static String[][] stats = {{"Simple and intuitive implementation", "Fast for creating tables & inserting rows"}, {"Slow for selecting, updating, and deleting rows", "Resizing can increase complexity sporadically"}, {"Without Charging: 186.739s", "With Charging: 110.890s (40.6% increase)"},{"200MB"}};

    public ALEngine(){
        super(name, stats);
    }


    public String getName() {
        return name;
    }

    public String[][] getStats() {
        return stats;
    }


    public String executeSQL(String query) {

        HashMap<String, Object> parsedSQL;
        String command = "";
        
        try { 
            parsedSQL = CustomParser.parseSQL(query); 
            command = (String) parsedSQL.get("command");
 
        } catch (Exception e) { 
            return e.getMessage(); 
        } 

        switch (command) {
            case "CREATE":
                return create(parsedSQL);
            case "INSERT":
                return insert(parsedSQL);
            case "SELECT":
                return select(parsedSQL);
            case "UPDATE":
                return update(parsedSQL);
            case "DELETE":
                return delete(parsedSQL);
            default:
                return "ERROR: Unknown command";
        }
    }
    
    public String create(HashMap<String, Object> parsedSQL) {

        String tableName = (String) parsedSQL.get("tableName");
        List<String> columnNames = (List<String>) parsedSQL.get("columns");

        Table table = new Table(tableName, columnNames);
        // table.selectAllRows();

        schema.addTable(table);
        // schema.printSchema();
        String result = String.format("'%s' table created.", tableName);
        return result;
    }

    public String insert(HashMap<String, Object> parsedSQL) {
        
        String tableName = (String) parsedSQL.get("tableName");
        Table table = schema.getTableByName(tableName);

        List<Object> valueList = (List<Object>) parsedSQL.get("columns");
        table.insertRow(valueList);
        
        return "INSERT successful";
    }

    public String select(HashMap<String, Object> parsedSQL) {
        
        String tableName = (String) parsedSQL.get("tableName");
        Table table = schema.getTableByName(tableName);
        
        Object target = parsedSQL.get("target");

        if (target.equals("*")) { // All columns

            if (!parsedSQL.containsKey("whereOperator")) { // Zero conditions
                return table.selectAllRows();
            
            } else {
                if (!parsedSQL.containsKey("secondCondition")) { // One condition
                    String whereConditionColumn = (String) parsedSQL.get("whereConditionColumn");
                    String whereOperator = (String) parsedSQL.get("whereOperator");
                    Object whereValue = parsedSQL.get("whereValue");

                    whereValue = convertToCorrectType(whereValue);

                    Condition condition = ConditionBuilder.buildCondition(whereConditionColumn, whereOperator, whereValue, table);

                    return table.selectRows(condition);
                    

                } else { // Two conditions
                    String whereConditionColumn = (String) parsedSQL.get("whereConditionColumn");
                    String whereOperator = (String) parsedSQL.get("whereOperator");
                    Object whereValue = parsedSQL.get("whereValue");

                    String secondCondition = (String) parsedSQL.get("secondCondition");

                    String secondConditionColumn = (String) parsedSQL.get("secondConditionColumn");
                    String secondOperator = (String) parsedSQL.get("secondOperator");
                    Object secondValue = parsedSQL.get("secondValue");

                    whereValue = convertToCorrectType(whereValue);
                    secondValue = convertToCorrectType(secondValue);

                    Condition condition1 = ConditionBuilder.buildCondition(whereConditionColumn, whereOperator, whereValue, table);
                    Condition condition2 = ConditionBuilder.buildCondition(secondConditionColumn, secondOperator, secondValue, table);

                    secondCondition = secondCondition.toUpperCase();

                    Condition combinedCondition;
                    if (secondCondition.equals("AND")) {
                        combinedCondition = condition1.and(condition2);
                    } else if (secondCondition.equals("OR")) {
                        combinedCondition = condition1.or(condition2);
                    } else {
                        throw new IllegalArgumentException("Condition operator " + secondCondition + " is not valid");
                    }

                    return table.selectRows(combinedCondition);

                }
            }
        } else { // Specific columns

            if (!parsedSQL.containsKey("whereOperator")) { // Zero conditions

                List<String> selectedColumns = (List<String>) parsedSQL.get("target");
                
                return table.selectColumns(selectedColumns);

            } else {
                if (!parsedSQL.containsKey("secondCondition")) { // One condition
                    String whereConditionColumn = (String) parsedSQL.get("whereConditionColumn");
                    String whereOperator = (String) parsedSQL.get("whereOperator");
                    Object whereValue = parsedSQL.get("whereValue");

                    whereValue = convertToCorrectType(whereValue);

                    Condition condition = ConditionBuilder.buildCondition(whereConditionColumn, whereOperator, whereValue, table);

                    List<String> selectedColumns = (List<String>) parsedSQL.get("target");
                
                    return table.selectColumnsWithCondition(selectedColumns, condition);                    

                } else { // Two conditions
                    String whereConditionColumn = (String) parsedSQL.get("whereConditionColumn");
                    String whereOperator = (String) parsedSQL.get("whereOperator");
                    Object whereValue = parsedSQL.get("whereValue");

                    String secondCondition = (String) parsedSQL.get("secondCondition");

                    String secondConditionColumn = (String) parsedSQL.get("secondConditionColumn");
                    String secondOperator = (String) parsedSQL.get("secondOperator");
                    Object secondValue = parsedSQL.get("secondValue");

                    whereValue = convertToCorrectType(whereValue);
                    secondValue = convertToCorrectType(secondValue);

                    Condition condition1 = ConditionBuilder.buildCondition(whereConditionColumn, whereOperator, whereValue, table);
                    Condition condition2 = ConditionBuilder.buildCondition(secondConditionColumn, secondOperator, secondValue, table);

                    secondCondition = secondCondition.toUpperCase();

                    Condition combinedCondition;
                    if (secondCondition.equals("AND")) {
                        combinedCondition = condition1.and(condition2);
                    } else if (secondCondition.equals("OR")) {
                        combinedCondition = condition1.or(condition2);
                    } else {
                        throw new IllegalArgumentException("Condition operator " + secondCondition + " is not valid");
                    }

                    List<String> selectedColumns = (List<String>) parsedSQL.get("target");
                
                    return table.selectColumnsWithCondition(selectedColumns, combinedCondition);
                }
            }
        }
    }

    public String update(HashMap<String, Object> parsedSQL) {
        
        String tableName = (String) parsedSQL.get("tableName");
        Table table = schema.getTableByName(tableName);

        Object target = parsedSQL.get("target");
        List<String> selectedColumns = (List<String>) parsedSQL.get("target");
        
        Map<String, Object> newValues = new HashMap<>();
        for (int i = 0; i < selectedColumns.size(); i+=2) {
            newValues.put(selectedColumns.get(i), selectedColumns.get(i+1));
        }

        if (!parsedSQL.containsKey("secondCondition")) { // One condition
            
            String whereConditionColumn = (String) parsedSQL.get("whereConditionColumn");
            String whereOperator = (String) parsedSQL.get("whereOperator");
            Object whereValue = parsedSQL.get("whereValue");

            whereValue = convertToCorrectType(whereValue);

            Condition condition = ConditionBuilder.buildCondition(whereConditionColumn, whereOperator, whereValue, table);

            table.updateRows(condition, newValues);

        } else { // Two conditions

            String whereConditionColumn = (String) parsedSQL.get("whereConditionColumn");
            String whereOperator = (String) parsedSQL.get("whereOperator");
            Object whereValue = parsedSQL.get("whereValue");

            whereValue = convertToCorrectType(whereValue);

            String secondCondition = (String) parsedSQL.get("secondCondition");

            String secondConditionColumn = (String) parsedSQL.get("secondConditionColumn");
            String secondOperator = (String) parsedSQL.get("secondOperator");
            Object secondValue = parsedSQL.get("secondValue");

            secondValue = convertToCorrectType(secondValue);

            Condition condition1 = ConditionBuilder.buildCondition(whereConditionColumn, whereOperator, whereValue, table);
            Condition condition2 = ConditionBuilder.buildCondition(secondConditionColumn, secondOperator, secondValue, table);

            secondCondition = secondCondition.toUpperCase();

            Condition combinedCondition;
            if (secondCondition.equals("AND")) {
                combinedCondition = condition1.and(condition2);
            } else if (secondCondition.equals("OR")) {
                combinedCondition = condition1.or(condition2);
            } else {
                throw new IllegalArgumentException("Condition operator " + secondCondition + " is not valid");
            }

            table.updateRows(combinedCondition, newValues);
        }

        return "UPDATE command executed";
    }

    public String delete(HashMap<String, Object> parsedSQL) {
        
        String tableName = (String) parsedSQL.get("tableName");
        Table table = schema.getTableByName(tableName);

        if (!parsedSQL.containsKey("whereOperator")) { // Zero conditions
            table.deleteAllRows();

        } else {

            if (!parsedSQL.containsKey("secondCondition")) { // One condition
            
                String whereConditionColumn = (String) parsedSQL.get("whereConditionColumn");
                String whereOperator = (String) parsedSQL.get("whereOperator");
                Object whereValue = parsedSQL.get("whereValue");
                
                whereValue = convertToCorrectType(whereValue);

                Condition condition = ConditionBuilder.buildCondition(whereConditionColumn, whereOperator, whereValue, table);
    
                table.deleteRows(condition);
    
            } else { // Two conditions
    
                String whereConditionColumn = (String) parsedSQL.get("whereConditionColumn");
                String whereOperator = (String) parsedSQL.get("whereOperator");
                Object whereValue = parsedSQL.get("whereValue");
                whereValue = convertToCorrectType(whereValue);
    
                String secondCondition = (String) parsedSQL.get("secondCondition");
    
                String secondConditionColumn = (String) parsedSQL.get("secondConditionColumn");
                String secondOperator = (String) parsedSQL.get("secondOperator");
                Object secondValue = parsedSQL.get("secondValue");
                secondValue = convertToCorrectType(secondValue);
    
                Condition condition1 = ConditionBuilder.buildCondition(whereConditionColumn, whereOperator, whereValue, table);
                Condition condition2 = ConditionBuilder.buildCondition(secondConditionColumn, secondOperator, secondValue, table);
    
                secondCondition = secondCondition.toUpperCase();

                Condition combinedCondition;
                if (secondCondition.equals("AND")) {
                    combinedCondition = condition1.and(condition2);
                } else if (secondCondition.equals("OR")) {
                    combinedCondition = condition1.or(condition2);
                } else {
                    throw new IllegalArgumentException("Condition operator " + secondCondition + " is not valid");
                }
    
                table.deleteRows(combinedCondition);
            }
        }

        return "DELETE command executed";
    }

    public static Object convertToCorrectType(Object input_value) {
        // Ensure input_value is a String
        String value;
        if (input_value instanceof String) {
            value = (String) input_value;
        } else {
            value = input_value.toString();
        }
    
        // Check for Boolean
        if (value.equalsIgnoreCase("true")) {
            return Boolean.TRUE;
        }
        if (value.equalsIgnoreCase("false")) {
            return Boolean.FALSE;
        }
    
        // Check for Double (covers both Integer and Decimal values as Double)
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            // Not a Double
        }
    
        // If all checks fail, treat it as a String
        return value;
    }
    
}


