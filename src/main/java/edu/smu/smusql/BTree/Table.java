package edu.smu.smusql.BTree;

import java.util.*;

public class Table {
    private String name;
    private ArrayList<BTree> columns = new ArrayList<>();
    private ArrayList<String> columnHeaders;
    private Integer Id = 1; 


    public Table(String name, String[] cols){
        this.name = name;

        columnHeaders = new ArrayList<>(Arrays.asList(cols));

        for (String s : cols) {
            BTree colBTree = new BTree(3);
            columns.add(colBTree);
        }
        
    }

    public void insert(String[] rowData){

        for(int i = 0; i < columns.size(); i++){
            BTree tempTree = columns.get(i);


            //determine if rowData is Integer or String
            if(TableUtils.isInteger(rowData[i])){
                tempTree.insert(new KeyValuePair<Integer,Integer>(Id, Integer.parseInt(rowData[i])));
               
            }else if (TableUtils.isDouble(rowData[i])){
                tempTree.insert(new KeyValuePair<Integer,Double>(Id, Double.parseDouble(rowData[i])));
            }else{
                tempTree.insert(new KeyValuePair<Integer,String>(Id, rowData[i]));
            }
        }
        Id++;
    }

    public String update(String[] newValues, String[][] condition, String type){
        List<Integer> affectedRows = selectCondition(condition, type);

        //get which columns to update
        List<Integer> affectedColumns = new ArrayList<>();

        for(int i = 0; i < newValues.length; i++){
            if(newValues[i].length() > 0){
                affectedColumns.add(i);
            }
        }

        for(Integer i : affectedRows){
            
            for(Integer j : affectedColumns){
                BTree temp = columns.get(j);

                if(TableUtils.isInteger(newValues[j])){
                    temp.update(i, Integer.parseInt(newValues[j]));
                }else if(TableUtils.isDouble(newValues[j])){
                    temp.update(i, Double.parseDouble(newValues[j]));
                }else{
                    temp.update(i, newValues[j]);
                }

                
            }
            
        }

        // System.out.println("conditions: " + Arrays.asList(condition) + " type: " + type + "affected Rows:" + affectedRows.size());
        return "Successfully deleted " + affectedRows.size() + " rows";
    }

    public String delete(String[][] condition, String type){
        List<Integer> rowsToDelete = selectCondition(condition, type);

        // System.out.println("rows to delete" + rowsToDelete);

        for(Integer i : rowsToDelete){

            for(BTree b : columns){

                b.delete(i);

                // System.out.println(b.search(i));
            }
        }

        // System.out.println("conditions: " + Arrays.asList(condition) + " type: " + type + "deleted Rows:" + rowsToDelete.size());
        return "Successfully deleted " + rowsToDelete.size() + " rows";
    }

    public List<Integer> selectCondition(String[][] condition, String type){
        List<List<Object>> result = new ArrayList<>();

        for(int i = 0; i < condition.length; i++){
            String[] currCondition = condition[i];

            boolean all = false;
            for(String s : currCondition){
                if(!s.isBlank()){
                    all = false;
                    break;
                }
            }

            if(all){
                List<Object> IDs = new ArrayList<Object>();

                for(int k = 1; k < Id; k++){
                    IDs.add(k);
                }

                result.add(IDs);
                continue;
            }

            for(int j = 0; j < currCondition.length; j++){
                String curr = currCondition[j];
    
    
                if (curr.length() == 0) {
                    continue;
                }
    
                BTree temp = columns.get(j);
    
                // temp.printBTree();
                ReversedBTree reverseTemp = temp.reverse();
                
                // reverseTemp.printBTree();
    
                List<Object> search = new ArrayList<>();
    
                if(curr.contains(">") || curr.contains("<")){
    
                    int substringLength = 1;
                    if(curr.charAt(1) == '='){
                        substringLength = 2;
                    }
    
                    String operator = curr.substring(0, substringLength);
    
                    if(TableUtils.isInteger(curr.substring(substringLength))){
                        Integer value = Integer.parseInt(curr.substring(substringLength));
    
                        List<Object> searchResults = reverseTemp.searchByRange(value, operator);
    
                        if(searchResults != null){
                            search.addAll(reverseTemp.searchByRange(value, operator));
                        }
                        
                        
                    }else{
                        Double value = Double.parseDouble(curr.substring(substringLength));
    
                        List<Object> searchResults = reverseTemp.searchByRange(value, operator);
    
                        if(searchResults != null){
                            search.addAll(reverseTemp.searchByRange(value, operator));
                        }
                    }
                    
                }else{
                    //find data type
                    if (TableUtils.isInteger(curr)) {
    
                        search.addAll(reverseTemp.searchByValue(Integer.parseInt(curr)));
                    }else if(TableUtils.isDouble(curr)){
                        search.addAll(reverseTemp.searchByValue(Double.parseDouble(curr)));
                    }else{
                        search.addAll(reverseTemp.searchByValue(curr));
    
                        // System.out.println(reverseTemp.searchByValue(curr));
                    }
                    
                }
    
                
    
                result.add(search);
            }

        }

        
        // System.out.println("result" + result);
        List<Object> temp = new ArrayList<>();

        if(type.equalsIgnoreCase("AND")){
            temp = TableUtils.findIntersection(result);
        }else if (type.equalsIgnoreCase("OR")){
            temp = TableUtils.findUnion(result);
        }else{
            temp = result.getFirst();
        }


        List<Integer> IDs = TableUtils.convertToIntegerList(temp);
        return IDs;
    }

    public String selectConditionPrint(String[][] condition, String type, String[] cols){
        List<Integer> IDs = selectCondition(condition, type);

        // System.out.println("IDs" + IDs);

        // System.out.print("conditions: ");
        // for(String[] s : condition){
        //     System.out.print(Arrays.asList(s));
        // }
        // System.out.println(" type: " + type);


        String returnStr = printByID(IDs, cols);
        return returnStr;
    }

    public String selectAll(){
        StringBuilder output = new StringBuilder();
    
        // Add table name and column headers
        output.append("Table: ").append(name).append("\n");
        output.append("Row ID\t");
    
        for (String s : columnHeaders) {
            output.append(s).append("\t");
        }
    
        output.append("\n");
    
        // Iterate through rows and print data for each column
        for (int i = 1; i < Id; i++) {
            output.append(i).append("\t");
    
            for (BTree b : columns) {
                output.append(b.search(i)).append("\t");
            }
    
            output.append("\n");
        }
    
        // Convert StringBuilder to String and return
        return output.toString();
    }

    public List<Object> getRowByID(Integer ID){
        List<Object> rowData = new ArrayList<>();
        
        for(BTree b : columns){
            rowData.add(b.search(ID));
        }

        return rowData;
    }

    public String printByID(List<Integer> IDArray, String[] cols){
        StringBuilder output = new StringBuilder();

        // Add table name
        output.append("Table: ").append(name).append("\n");
        output.append("Row ID\t");

        List<Integer> columnIDs = new ArrayList<Integer>();

        if (cols[0].equals("*")) {
            // Print all column headers if cols[0] is "*"
            for (String s : columnHeaders) {
                output.append(s).append("\t");
            }
        } else {
            // Find column IDs for specific column names
            for (String s : cols) {
                for (int i = 0; i < columnHeaders.size(); i++) {
                    if (columnHeaders.get(i).trim().equals(s.trim())) {
                        columnIDs.add(i);
                    }
                }
            }

            // Append selected column headers
            for (String s : cols) {
                output.append(s).append("\t");
            }
        }

        output.append("\n");

        // Iterate over rows (IDs)
        for (Integer i : IDArray) {
            output.append(i).append("\t");

            // If selecting all columns
            if (cols[0].equals("*")) {
                for (BTree b : columns) {
                    output.append(b.search(i)).append("\t");
                }
            } else {
                // If selecting specific columns
                for (int j = 0; j < columnIDs.size(); j++) {
                    int currID = columnIDs.get(j);
                    output.append(columns.get(currID).search(i)).append("\t");
                }
            }

            output.append("\n");
        }

        // Convert StringBuilder to String and return
        return output.toString();
    }


    public ArrayList<String> getColumnHeaders() {
        return columnHeaders;
    }

    public ArrayList<BTree> getColumns() {
        return columns;
    }

}
