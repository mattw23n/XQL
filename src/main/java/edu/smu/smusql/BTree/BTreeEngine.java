package edu.smu.smusql.BTree;

import java.util.*;

import edu.smu.smusql.CustomParser;
import edu.smu.smusql.Engine;

public class BTreeEngine extends Engine {
    private BTree dBTree = new BTree(3);
    private static String name = "BTreeCols";
    private static String[][] stats = {{"Excellent for range-based searches"}, {"Column-first focus is less efficient for row-based SQL operations, requiring additional traversal"}, {"Conditional operations remain constly O(N)"}, {"Without Charging: 154.410s", "With Charging: 83.648s (45.8% increase)"}, {"150 MB"}};

    public BTreeEngine(){
        super(name, stats);
    }


    public String getName() {
        return name;
    }

    public String[][] getStats() {
        return stats;
    }

    
    public String create(HashMap<String, Object> map){
        String tableName = (String) map.get("tableName");
        List<String> colArrList = (List<String>) map.get("columns");

        String[] columns = colArrList.toArray(new String[0]);

        Table newTable = new Table(tableName, columns);

        KeyValuePair TableIDPair = new KeyValuePair<String,Table>(tableName, newTable);
        dBTree.insert(TableIDPair);

        // System.out.println(Arrays.toString(columns));

        return "Table " + tableName + " created successfully!";
    }

    public Table selectTable(String tableName){
        Table returnTable = (Table) dBTree.search(tableName);
        if(returnTable == null){
            // System.out.println("Table does not exist!");
            return null;
        }
        
        return returnTable;
    }

    public String insert(HashMap<String, Object> map){
        String tableName = (String) map.get("tableName");

        List<String> colArrList = (List<String>) map.get("columns");

        String[] newValues = colArrList.toArray(new String[0]);

        Table table = selectTable(tableName);

        if(table == null){
            return  "Table does not exist";
        }

        table.insert(newValues);
        // System.out.println(Arrays.toString(newValues) +  "inserted into TABLE" + tableName);
        return Arrays.toString(newValues) +  "inserted into TABLE" + tableName;
        
    }

    public String select(HashMap<String, Object> map){
        String returnStr = "";
        String tableName = (String) map.get("tableName");
        Object target = map.get("target");
        String targetStr = "";
        String[] targetArr = null;
        String[] selectAll = {"*"};

        // for (String key : map.keySet()) {
        //     System.out.println("Key: " + key + ", Value: " + map.get(key));
        // }

        //check target
        if(target instanceof String){
            targetStr = (String) target;
        }else{
            List<String> colArrList = (List<String>) target;
            String[] newValues = colArrList.toArray(new String[0]);

            targetArr = newValues;
        }

        //get table
        Table table = selectTable(tableName);

        if(table == null){
            return "Table does not exist";
        }

        //check if have where
        boolean isWhereExists = (map.get("whereOperator") == null ) ? false : true; 
        boolean isSecondCondition = (map.get("secondOperator") == null) ? false : true;



        ArrayList<String> cols = table.getColumnHeaders();
        List<String[]> conditionsList = new ArrayList<>();

        String[] firstCondition = new String[cols.size()];
        String[] secondCondition = new String[cols.size()];
        String type = "";

        for(int i = 0; i < firstCondition.length; i++){
            firstCondition[i] = "";
            secondCondition[i] = "";
        }

        if(isWhereExists){
            String whereColumn = (String) map.get("whereConditionColumn");
            String whereValue = (String) map.get("whereValue");
            String whereOperator = (String) map.get("whereOperator");
            
           

            for(int i = 0; i < cols.size(); i++){
                if(cols.get(i).trim().equals(whereColumn)){
                    String prefix = (whereOperator.equals("=")) ? "" : whereOperator;

                    firstCondition[i] = prefix + whereValue;
                }
            }

            conditionsList.add(firstCondition);

        }

        if(isSecondCondition){
            type += (String) map.get("secondCondition");
            String whereColumn = (String) map.get("secondConditionColumn");
            String whereValue = (String) map.get("secondValue");
            String whereOperator = (String) map.get("secondOperator");
        
            for(int i = 0; i < cols.size(); i++){
                if(cols.get(i).trim().equals(whereColumn)){
                    String prefix = (whereOperator.equals("=")) ? "" : whereOperator;

                    secondCondition[i] = prefix + whereValue;
                }
            }

            conditionsList.add(secondCondition);
        }

        String[][] conditions = conditionsList.toArray(new String[0][0]);

        // System.out.println(Arrays.toString(targetArr));

        // for(String[] s : conditions){
        //     System.out.println(Arrays.toString(s));
        // }

        // System.out.println(targetStr);

        
        if(!targetStr.equals("*") || isWhereExists){
            returnStr =  table.selectConditionPrint(conditions, type, (targetArr == null ? selectAll : targetArr));
        }else{
            returnStr = table.selectAll();
        }

        // System.out.println(returnStr);

        return returnStr;
    }

    public String update(HashMap<String, Object> map){
        // for (String key : map.keySet()) {
        //     System.out.println("Key: " + key + ", Value: " + map.get(key));
        // }
        // System.out.println();

        String tableName = (String) map.get("tableName");
        //get table
        Table table = selectTable(tableName);

        if(table == null){
            return "Table does not exist";
        }



        List<String> colArrList = (List<String>) map.get("target");;
        String[] targetArr = colArrList.toArray(new String[0]);

        //check if have where
        boolean isWhereExists = (map.get("whereOperator") == null ) ? false : true; 
        boolean isSecondCondition = (map.get("secondOperator") == null) ? false : true;

        ArrayList<String> cols = table.getColumnHeaders();

        String[] newValues = new String[cols.size()];

        List<String[]> conditionsList = new ArrayList<>();
        String[] firstCondition = new String[cols.size()];
        String[] secondCondition = new String[cols.size()];

        String type = "";

        for(int i = 0; i < firstCondition.length; i++){
            firstCondition[i] = "";
            secondCondition[i] = "";
            newValues[i] = "";
        }

        if(isWhereExists){
            String whereColumn = (String) map.get("whereConditionColumn");
            String whereValue = (String) map.get("whereValue");
            String whereOperator = (String) map.get("whereOperator");
            
           

            for(int i = 0; i < cols.size(); i++){
                if(cols.get(i).trim().equals(whereColumn)){
                    String prefix = (whereOperator.equals("=")) ? "" : whereOperator;

                    firstCondition[i] = prefix + whereValue;
                }
            }

            conditionsList.add(firstCondition);

        }

        if(isSecondCondition){
            type += (String) map.get("secondCondition");
            String whereColumn = (String) map.get("secondConditionColumn");
            String whereValue = (String) map.get("secondValue");
            String whereOperator = (String) map.get("secondOperator");
        
            for(int i = 0; i < cols.size(); i++){
                if(cols.get(i).trim().equals(whereColumn)){
                    String prefix = (whereOperator.equals("=")) ? "" : whereOperator;

                    secondCondition[i] = prefix + whereValue;
                }
            }

            conditionsList.add(secondCondition);
        }

        String[][] conditions = conditionsList.toArray(new String[0][0]);

        
        //create newValues array
        for(int i = 0; i < cols.size(); i++){

            String currCol = cols.get(i).trim();

            for(int j = 0; j < targetArr.length; j+= 2){
                String colToBeUpdated = targetArr[j];
                String updatedValue = targetArr[j + 1];

                if(currCol.equals(colToBeUpdated)){
                    newValues[i] = updatedValue;
                }

            }
            
        }

        // System.out.println(Arrays.toString(newValues));

        // for(String[] s : conditions){
        //     System.out.println(Arrays.toString(s));
        // }

        String returnStr = table.update(newValues, conditions, type);

        return returnStr;
    }


    public String delete(HashMap<String, Object> map){
        // for (String key : map.keySet()) {
        //     System.out.println("Key: " + key + ", Value: " + map.get(key));
        // }
        // System.out.println();

        String tableName = (String) map.get("tableName");
        Table table = selectTable(tableName);

        if(table == null){
            return "Table does not exist";
        }

        //check if have where
        boolean isWhereExists = (map.get("whereOperator") == null ) ? false : true; 
        boolean isSecondCondition = (map.get("secondOperator") == null) ? false : true;

        ArrayList<String> cols = table.getColumnHeaders();
        List<String[]> conditionsList = new ArrayList<>();

        String[] firstCondition = new String[cols.size()];
        String[] secondCondition = new String[cols.size()];
        String type = "";

        for(int i = 0; i < firstCondition.length; i++){
            firstCondition[i] = "";
            secondCondition[i] = "";
        }

        if(isWhereExists){
            String whereColumn = (String) map.get("whereConditionColumn");
            String whereValue = (String) map.get("whereValue");
            String whereOperator = (String) map.get("whereOperator");
            
           

            for(int i = 0; i < cols.size(); i++){
                if(cols.get(i).trim().equals(whereColumn)){
                    String prefix = (whereOperator.equals("=")) ? "" : whereOperator;

                    firstCondition[i] = prefix + whereValue;
                }
            }

            conditionsList.add(firstCondition);

        }

        if(isSecondCondition){
            type += (String) map.get("secondCondition");
            String whereColumn = (String) map.get("secondConditionColumn");
            String whereValue = (String) map.get("secondValue");
            String whereOperator = (String) map.get("secondOperator");
        
            for(int i = 0; i < cols.size(); i++){
                if(cols.get(i).trim().equals(whereColumn)){
                    String prefix = (whereOperator.equals("=")) ? "" : whereOperator;

                    secondCondition[i] = prefix + whereValue;
                }
            }

            conditionsList.add(secondCondition);
        }

        String[][] conditions = conditionsList.toArray(new String[0][0]);

        // for(String[] s : conditions){
        //     System.out.println(Arrays.toString(s));
        // }
        // System.out.println(type);

        String returnStr = table.delete(conditions, type);
        // System.out.println(returnStr);
        return returnStr;
    }


    public String executeSQL(String query) {
        HashMap<String, Object> map;
        String command = "";
        
        try { 
            map = CustomParser.parseSQL(query); 
            command = (String) map.get("command");
 
        } catch (Exception e) { 
            return e.getMessage(); 
        } 

        switch (command) {
            case "CREATE":
                return create(map);
            case "INSERT":
                // System.out.println(query);
                return insert(map);
            case "SELECT":
                return select(map);
            case "UPDATE":
                // System.out.println(query);
                return update(map);
            case "DELETE":
                // System.out.println(query);
                return delete(map);
            default:
                return "ERROR: Unknown command";
        }
    }
    
}

