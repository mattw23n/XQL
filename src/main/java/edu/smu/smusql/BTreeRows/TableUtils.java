package edu.smu.smusql.BTreeRows;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class TableUtils {

    private TableUtils(){

    }

    public static String[] convertToStringArray(List<Object> objectList) {
        // Create a new String array with the same size as the list
        String[] stringArray = new String[objectList.size()];

        // Iterate through the list and convert each element to a String
        for (int i = 0; i < objectList.size(); i++) {
            stringArray[i] = objectList.get(i).toString();  // Use toString() to convert the object to a String
        }

        return stringArray;
    }
    
    public static List<Integer> convertToIntegerList(List<Object> objectList) {
        List<Integer> integerList = new ArrayList<>();
        
        for (Object obj : objectList) {
            if (obj instanceof Integer) { // Check if the object is an Integer before casting
                integerList.add((Integer) obj);
            } else {
                System.out.println("Warning: Non-integer object found: " + obj);
            }
        }

        return integerList;
    }

    // Method to find the intersection of a List of Lists
    public static List<Object> findIntersection(List<List<Object>> listOfLists) {
        // If the input is null or there are no lists, return an empty list
        if (listOfLists == null || listOfLists.isEmpty()) {
            return new ArrayList<>();
        }

        // Initialize the intersection list with the first list
        List<Object> intersection = new ArrayList<>(listOfLists.get(0));

        // Iterate through the rest of the lists and retain only common elements
        for (int i = 1; i < listOfLists.size(); i++) {
            List<Object> currentList = listOfLists.get(i);

            // If any list is null or empty, the intersection will be empty
            if (currentList == null || currentList.isEmpty()) {
                return new ArrayList<>();
            }

            // Use retainAll() to keep only elements that are in both the intersection and the current list
            intersection.retainAll(currentList);

            // If intersection becomes empty, we can return early
            if (intersection.isEmpty()) {
                return intersection;
            }
        }

        return intersection;
    }

    public static boolean isInteger(String str) {
        try {
            Integer.parseInt(str);
            return true;  // If parsing is successful, it's an integer
        } catch (NumberFormatException e) {
            return false; // If parsing fails, it's not an integer
        }
    }

    public static boolean isDouble(String str) {
        try {
            Double.parseDouble(str);
            return true;  // If parsing is successful, it's an integer
        } catch (NumberFormatException e) {
            return false; // If parsing fails, it's not an integer
        }
    }

    // Method to return the OR result (union) of a list of List<Object>
    public static List<Object> findUnion(List<List<Object>> listOfLists) {
        // Create a Set to store unique elements (OR operation)
        Set<Object> resultSet = new HashSet<>();

        // Iterate through each list in the list of lists
        for (List<Object> list : listOfLists) {
            if (list != null) {
                // Add all elements from the current list to the resultSet (duplicates are ignored)
                resultSet.addAll(list);
            }
        }

        // Convert the resultSet back to a List and return it
        return new ArrayList<>(resultSet);
    }


}
