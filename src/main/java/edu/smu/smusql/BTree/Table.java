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

    public void update(String[] newValues, String[] condition, String type){
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
    }

    public void delete(String[] condition, String type){
        List<Integer> rowsToDelete = selectCondition(condition, type);

        // System.out.println("rows to delete" + rowsToDelete);

        for(Integer i : rowsToDelete){

            for(BTree b : columns){

                b.delete(i);

                // System.out.println(b.search(i));
            }
        }

        // System.out.println("conditions: " + Arrays.asList(condition) + " type: " + type + "deleted Rows:" + rowsToDelete.size());
        
    }

    public List<Integer> selectCondition(String[] condition, String type){
        List<List<Object>> result = new ArrayList<>();

        boolean all = true;
        for(int i = 0; i < condition.length; i++){
            if(!condition[i].isBlank()){
                all = false;
            }
        }

        if(all){
            List<Integer> IDs = new ArrayList<Integer>();

            for(int i =1; i < Id; i++){
                IDs.add(i);
            }

            return IDs;
        }

        
        for(int i = 0; i < condition.length; i++){
            String curr = condition[i];


            if (curr.length() == 0) {
                continue;
            }

            BTree temp = columns.get(i);

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

                    // System.out.println(reverseTemp.searchByValue(Integer.parseInt(curr)));

                    search.addAll(reverseTemp.searchByValue(Integer.parseInt(curr)));
                }else if(TableUtils.isDouble(curr)){
                    search.addAll(reverseTemp.searchByValue(Double.parseDouble(curr)));
                }else{
                    search.addAll(reverseTemp.searchByValue(curr));
                }
                
            }

            

            result.add(search);
        }

        // System.out.println(result);
        List<Object> temp = new ArrayList<>();

        if(type.equals("AND")){
            temp = TableUtils.findIntersection(result);
        }else if (type.equals("OR")){
            temp = TableUtils.findUnion(result);
        }else{
            temp = result.getFirst();
        }


        List<Integer> IDs = TableUtils.convertToIntegerList(temp);
        return IDs;
    }

    public void selectConditionPrint(String[] condition, String type, String[] cols){
        List<Integer> IDs = selectCondition(condition, type);

        System.out.println("conditions: " + Arrays.asList(condition) + " type: " + type);
        printByID(IDs, cols);
    }

    public void selectAll(){
        System.out.println("Table: " + name);
        System.out.print("Row ID\t");

        for(String s : columnHeaders){
            System.out.print(s + "\t");
        }

        System.out.println();

        for(int i = 1; i < Id; i++){
            System.out.print(i + "\t");

            for(BTree b : columns){
                System.out.print(b.search(i) + "\t");
            }

            System.out.println();

            }

        System.out.println();
    }

    public List<Object> getRowByID(Integer ID){
        List<Object> rowData = new ArrayList<>();
        
        for(BTree b : columns){
            rowData.add(b.search(ID));
        }

        return rowData;
    }

    public void printByID(List<Integer> IDArray, String[] cols){
        System.out.println("Table: " + name);
        System.out.print("Row ID\t");

        List<Integer> columnIDs = new ArrayList<Integer>();

            if(cols[0] == "*"){
                for(String s : columnHeaders){
                    System.out.print(s + "\t");
                }
                

            }else{
                for(String s : cols){
                    for(int i = 0; i < columnHeaders.size(); i++){

                        if(columnHeaders.get(i).trim().equals(s.trim())){
                            
                            columnIDs.add(i);
                        }
                    }

                    // columnIDs.add(columnHeaders.indexOf(s));
                    
                }
                

                for(String s : cols){
                    System.out.print(s + "\t");
                }

                
            }
            System.out.println();


            
            // System.out.println(columnIDs);
            for(Integer i : IDArray){
                System.out.print(i + "\t");

                if(cols[0] == "*"){
                    for(BTree b : columns){
                        System.out.print(b.search(i) + "\t");
                    }
                }else{
                    for(int j = 0; j < columnIDs.size(); j++){
                        int currID = columnIDs.get(j);
                        
                        System.out.print(columns.get(currID).search(i) + "\t");
                    }
                }
                

                System.out.println();
            }
            System.out.println();
        }


    public ArrayList<String> getColumnHeaders() {
        return columnHeaders;
    }

    public ArrayList<BTree> getColumns() {
        return columns;
    }

}
