package edu.smu.smusql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.HashMap;

//FOR SELECT & UPDATE & DELETE
/*
 * keys in response are
 * "command": SQL's action in the statement CREATE / INSERT / SELECT / DELETE
 * "tableName": name of table
 * "target": targetted columns. Either * as STRING or specified columns as LIST<STRING> 
 * "columns": list of values or columns within parenthesis for CREATE or INSERT statements
 * "whereConditionColumn": Column name of where condition
 * "whereOperator": operator of the where condition. Either = or < or >
 * "whereValue": value the where operator should check with
 * 
 * "secondCondition": the AND / OR statement.
 * "secondConditionColumn": Column name of AND / OR condition
 * "secondOperator": operator of the 2nd condition. Either = or < or >
 * "secondValue": value the 2nd condition operator should check with
 * 
 */

public class CustomParser {

    public static HashMap<String, Object> parseSQL(String query) {
        HashMap<String, Object> response = new HashMap<>();

        String[] tokens = query.trim().split("\\s+");
        String command = tokens[0].toUpperCase();

        switch (command) {

            case "CREATE":
                return parseCreate(tokens);

            case "INSERT":
                return parseInsert(tokens);

            case "SELECT":
                // table name
                int walk = 0;
                while (walk < tokens.length) {
                    if (tokens[walk].equalsIgnoreCase("FROM")) {
                        response.put("tableName", tokens[++walk]);// The name of the table to be inserted into.
                        break;
                    }
                    walk++;
                }

                // command
                response.put("command", command);

                // target
                int columnIndex = 1;
                if (tokens[1].equals("*")) {
                    response.put("target", tokens[1]);
                    columnIndex = 3; // to skip FROM and TABLE NAME
                } else {
                    ArrayList<String> columnsList = new ArrayList<>();

                    while (columnIndex < tokens.length && !tokens[columnIndex].equalsIgnoreCase("FROM")) {
                        if (tokens[columnIndex].equals(",")) { // Skip commas
                            columnIndex++;
                        }
                        columnsList.add(tokens[columnIndex].replace(",", ""));
                        columnIndex++;
                    }
                    response.put("target", columnsList);
                    columnIndex++;
                }

                if (columnIndex < tokens.length - 1 && tokens[++columnIndex].equalsIgnoreCase("WHERE")) {
                    columnIndex++;
                    response.put("whereConditionColumn", tokens[columnIndex++]);
                    response.put("whereOperator", tokens[columnIndex++]);
                    response.put("whereValue", tokens[columnIndex++]);
                }

                if (columnIndex < tokens.length
                        && (tokens[columnIndex].equalsIgnoreCase("AND")
                                || tokens[columnIndex].equalsIgnoreCase("OR"))) {
                    response.put("secondCondition", tokens[columnIndex++]);
                    response.put("secondConditionColumn", tokens[columnIndex++]);
                    response.put("secondOperator", tokens[columnIndex++]);
                    response.put("secondValue", tokens[columnIndex++]);
                }

                break;

            case "UPDATE":
                if (!tokens[2].equalsIgnoreCase("SET")) {
                    throw new RuntimeException("ERROR: Invalid UPDATE syntax. Please include 'SET' after table name");
                }
                // table name
                response.put("tableName", tokens[1]);// The name of the table to be inserted into.
                // command
                response.put("command", command);

                // get column to be updated & new value
                ArrayList<String> columnsToUpdate = new ArrayList<>();
                int setIndex = 3; // Start from the token after "SET"
                // Loop through tokens until the WHERE clause starts
                // for target
                while (setIndex < tokens.length && !tokens[setIndex].equalsIgnoreCase("WHERE")) {
                    // Skip commas
                    if (tokens[setIndex].equals(",")) {
                        setIndex++;
                    }
                    columnsToUpdate.add(tokens[setIndex]); // column name or value

                    setIndex++;
                    if (setIndex < tokens.length && tokens[setIndex].equals("=")) {
                        setIndex++; // Skip "="
                        columnsToUpdate.add(tokens[setIndex++].replace(",", "")); // column value
                    }
                }
                response.put("target", columnsToUpdate);

                // get where condition
                if (tokens[setIndex++].equalsIgnoreCase("WHERE")) {
                    response.put("whereConditionColumn", tokens[setIndex++]);
                    response.put("whereOperator", tokens[setIndex++]);
                    response.put("whereValue", tokens[setIndex++]);
                }

                // get and/or and next condition

                if (setIndex < tokens.length
                        && (tokens[setIndex].equalsIgnoreCase("AND") || tokens[setIndex].equalsIgnoreCase("OR"))) {
                    response.put("secondCondition", tokens[setIndex++]);
                    response.put("secondConditionColumn", tokens[setIndex++]);
                    response.put("secondOperator", tokens[setIndex++]);
                    response.put("secondValue", tokens[setIndex++]);
                }
                break;

            case "DELETE":

                // table name
                response.put("tableName", tokens[2]);// The name of the table to be inserted into.
                // command
                response.put("command", command);

                if (tokens.length > 4) {
                    response.put("whereConditionColumn", tokens[4]);
                    response.put("whereOperator", tokens[5]);
                    response.put("whereValue", tokens[6]);

                }

                if (tokens.length > 8) {
                    response.put("secondCondition", tokens[7]);
                    response.put("secondConditionColumn", tokens[8]);
                    response.put("secondOperator", tokens[9]);
                    response.put("secondValue", tokens[10]);
                }

                break;

            default:
                return null;
        }

        // Trim the query and split by spaces
        return response;
    }

    @SuppressWarnings("unchecked")
    public static HashMap<String, Object> parseCreate(String[] tokens) {
        HashMap<String, Object> response = new HashMap<>();
        response.put("tableName", tokens[2]);// The name of the table to be inserted into.
        response.put("command", "CREATE");

        String valueList = queryBetweenParentheses(tokens, 3); // Get values list between parentheses
        List<String> values = Arrays.asList(valueList.split(",")); // These are the values in the row to be inserted.
        values.replaceAll(String::trim); // Trim any spaces
        response.put("columns", values);

        return response;

    }

    public static HashMap<String, Object> parseInsert(String[] tokens) {
        HashMap<String, Object> response = new HashMap<>();
        response.put("tableName", tokens[2]);// The name of the table to be inserted into.
        response.put("command", "INSERT");

        String valueList = queryBetweenParentheses(tokens, 4); // Get values list between parentheses
        List<String> values = Arrays.asList(valueList.split(",")); // These are the values in the row to be inserted.
        values.replaceAll(String::trim); // Trim any spaces
        response.put("columns", values);

        return response;
    }

    public static String queryBetweenParentheses(String[] tokens, int startIndex) {
        StringBuilder result = new StringBuilder();
        for (int i = startIndex; i < tokens.length; i++) {
            result.append(tokens[i]).append(" ");
        }
        return result.toString().trim().replaceAll("\\(", "").replaceAll("\\)", "");
    }
}
