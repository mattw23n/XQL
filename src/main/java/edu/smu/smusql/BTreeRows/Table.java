package edu.smu.smusql.BTreeRows;
import java.util.*;

public class Table {
    private static String name;
    private static BTree rows = new BTree(3);
    private static ArrayList<String> columnHeaders;
    private Integer Id = 1; 


    public static ArrayList<String> getColumnHeaders() {
        return columnHeaders;
    }

    public Table(String name, String[] cols){
        Table.name = name;

        columnHeaders = new ArrayList<>(Arrays.asList(cols));
    }

    public String insert(String[] rowData){

        BTree temp = new BTree(3);
        for(int i = 0; i < columnHeaders.size(); i++){
            temp.insert(new KeyValuePair<String,Object>(columnHeaders.get(i), rowData[i]));
        }

        rows.insert(new KeyValuePair<Integer,BTree>(Id, temp));

        Id++;

        return (Arrays.toString(rowData) + " inserted into " + name);
    }

    public String update(String[] newValues, String[][] condition, String type){
        List<Integer> affectedRows = getAffectedIDs(condition, type);
        List<Integer> affectedColumns = new ArrayList<>();

        for(int i = 0; i < newValues.length; i++){
            if(!newValues[i].equals("")){
                affectedColumns.add(i);
            }
        }

        for(Integer i : affectedRows){
            BTree row = (BTree) rows.search(i);

            for(Integer j : affectedColumns){
                row.update(columnHeaders.get(j), newValues[j]);
            }
            

        }
        return("Successfully updated " + affectedRows.size() + " columns");
    }

    public String delete(String[][] condition, String type){
        List<Integer> affectedRows = getAffectedIDs(condition, type);

        for(Integer i : affectedRows){
            rows.delete(i);
        }

        return ("Successfully deleted " + affectedRows.size() + " rows");
    }

    public List<Integer> getAffectedIDs(String[][] condition, String type){
        List<List<Object>> result = new ArrayList<>();

        for(String[] curr : condition){
        
            List<Object> search = new ArrayList<>();

            for(int i = 0; i < curr.length; i++){
                String currStr = curr[i];

                String columnName = columnHeaders.get(i);


                for(int j = 1; j < Id; j++){
                    BTree row = (BTree) rows.search(j);

                    if(row == null){
                        continue;
                    }

                    String value = (String) row.search(columnName);

                    // System.out.println("curr value " + value);

                    if(currStr.contains(">") || currStr.contains("<")){

                        int operatorIndex = (currStr.charAt(1) == '=') ? 2 : 1;
        
                        String operator = currStr.substring(0, operatorIndex);
                        String valueStr = currStr.substring(operatorIndex);
                        int compareResult =  value.compareTo(valueStr);

                        
                        // System.out.println("compare value " + valueStr);
                        // System.out.println(value.compareTo(valueStr));

                    
                        if(currStr.contains("=") && compareResult == 0){
                            search.add(j);

                        }else if (operator.contains(">") && compareResult > 0){
                            search.add(j);

                        }else if (operator.contains("<") && compareResult < 0){
                            search.add(j);
                        }

                        
                    }else if (currStr.equals(value)){
                        // System.out.println("curr string" + currStr);
                        // System.out.println(j);
                        search.add(j);
                    }
                }
            }

            result.add(search);
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
        // System.out.println(IDs);
        return IDs;
    }
 
    public String select(String[][] condition, String type, String[] cols){
        
        List<Integer> affectedRows = getAffectedIDs(condition, type);

        boolean allCols = true;

        for(String s : cols){

            if(s.equals("*")){
                break;
            }

            if(!s.isEmpty()){
                allCols = false;
                break;
            }
        }

        if(allCols){
            return print(affectedRows, columnHeaders);
        }else{
            return print(affectedRows, Arrays.asList(cols));
        }

    }

    public String printAll(){
        StringBuilder output = new StringBuilder();

        output.append("Table: ").append(name).append("\n");
        for(String s : columnHeaders){
            output.append(s).append("\t");
        }
        output.append("\n");

        for(int i = 1; i < Id; i++){
            BTree bTree = (BTree) rows.search(i);

            for(String s : columnHeaders){

                if(bTree != null){
                    output.append(bTree.search(s));
    
                }else{
                    output.append("null");
                }
                
                output.append("\t");
            }
            output.append("\n");

        }

        return output.toString();
    }

    public static String print(List<Integer> affectedRows, List<String> columns){
        StringBuilder output = new StringBuilder();

        output.append("Table: ").append(name).append("\n");


        for(String s : columns){
            output.append(s).append("\t");
        }
        output.append("\n");

        for(Integer i : affectedRows){

            BTree bTree = (BTree) rows.search(i);

            for(String s : columnHeaders){

                if(bTree != null){
                    output.append(bTree.search(s));
    
                }else{
                    output.append("null");
                }
                
                output.append("\t");
            }
            output.append("\n");

        }

        return output.toString();

        
    }


    public static void Test2(){
        String[] cols = {"ID", "Name", "Age", "GPA"};
        String[] all = {"*"};

        String[] row1 = {"1", "Bob", "18", "3.1"};
        String[] row2 = {"2", "Bill", "16", "2.66"};
        String[] row3 = {"3", "Tom", "20", "3.9"};
        String[] row4 = {"4", "Bob", "15", "2.7"};

        String[][] condition = {{"", "", "", ">2.7"}, {"", "Bob", "", ""}};
        String[][] condition2 = {{"", "Bob", "", ""}};
        String[] newValues = {"", "", "", "3.6"};

        
        
        Table test = new Table("test", cols);

        test.insert(row1);
        test.insert(row2);
        test.insert(row3);
        test.insert(row4);


        test.printAll();

        test.select(condition, "AND", all);


        System.out.println();

        test.printAll();

        test.update(newValues, condition2, "");
        test.printAll();

        test.delete(condition2, "");
        test.printAll();
        
    }

    public static void main(String[] args) {
        Test2();


        
    }

    
    
}
