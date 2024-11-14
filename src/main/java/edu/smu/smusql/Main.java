package edu.smu.smusql;

import java.util.HashMap;
import java.util.Random;
import java.util.Scanner;

// @author ziyuanliu@smu.edu.sg

public class Main {
    /*55
    5
     *  Main method for accessing the command line interface of the database engine.
     *  MODIFICATION OF THIS FILE IS NOT RECOMMENDED!
     */
    static Engine selectedEngine = null;
    
    static HashMap<Integer, String> engines = new HashMap<>();
    

    public static void intro(){
        System.out.println("--------------------------------------");
        System.out.println("Welcome to XQL!");
        System.out.println("Before we begin, please select which Engine you would like to use!");
        System.out.println("Engines available:");
        System.out.println("ID \tEngine Name");
        for (Integer key : engines.keySet()) {
            System.out.println(key + "\t" + engines.get(key));
        }
        
        
        System.out.println("\nPlease enter the ID of the Engine you would like to use");
        System.out.println("--------------------------------------"); 
    }

    public static void engineStats(Engine engine){
        String name = engine.getName();
        String[][] stats = engine.getStats();

        System.out.println("--------------------------------------"); 
        System.out.println("Engine Name: " + name);
        System.out.println();
        

        String[] pros = stats[0];
        System.out.println("Pros:");
        
        for(String s : pros){
            System.out.println(s);
        }

        System.out.println(); 

        String[] cons = stats[1];
        System.out.println("Cons:");

        for(String s : cons){
            System.out.println(s);
        }

        System.out.println("--------------------------------------"); 

    }
    
    public static void main(String[] args) {
        engines.put(1, "Default");
        engines.put(2, "BTreeRows");
        engines.put(3, "BTreeCols");
        engines.put(4, "Treemap");
        engines.put(5, "ArrayList");
        engines.put(6, "HashMap");  
        engines.put(7, "MapHeapMap");
        engines.put(8, "CircularLinkedList");

        Scanner scanner = new Scanner(System.in);

        System.out.println("XQL version 1.0");
        System.out.println("Have fun, and good luck!");

        //select Engine
        intro();
        while(true) {
            System.out.print("xql>");
            String query = scanner.nextLine();
            String selectedEngineName;
            Integer selectedID;

            if (query.equalsIgnoreCase("exit")) {
                break;
            }else{
                try {
                    selectedID = Integer.parseInt(query.trim());
                } catch (Exception e) {
                    System.out.println("Invalid ID!");
                    continue;
                }
            }

            if(engines.get(selectedID) == null){
                System.out.println("Invalid ID!");
                continue;
            }
            //get engine
            
            selectedEngineName = engines.get(selectedID);
            System.out.println("Successfully chosen engine" + selectedEngineName);
            selectedEngine = EngineFactory.getEngine(selectedEngineName);

            engineStats(selectedEngine);
            break;
        }

        while (true) {
            System.out.print("xql> ");
            String query = scanner.nextLine();
            if (query.equalsIgnoreCase("exit")) {
                break;
            } else if (query.equalsIgnoreCase("evaluate")) {
                long startTime = System.nanoTime();
                autoEvaluate();
                long stopTime = System.nanoTime();
                long elapsedTime = stopTime - startTime;
                double elapsedTimeInSecond = (double) elapsedTime / 1_000_000_000;
                System.out.println("Time elapsed: " + elapsedTimeInSecond + " seconds");
                break;
            }

            System.out.println(selectedEngine.executeSQL(query));
        }
        scanner.close();
    }


    /*
     *  Below is the code for auto-evaluating your work.
     *  DO NOT CHANGE ANYTHING BELOW THIS LINE!
     */
    public static void autoEvaluate() {

        // Set the number of queries to execute
        int numberOfQueries = 100000;

        // Create tables
        selectedEngine.executeSQL("CREATE TABLE users (id, name, age, city)");
        selectedEngine.executeSQL("CREATE TABLE products (id, name, price, category)");
        selectedEngine.executeSQL("CREATE TABLE orders (id, user_id, product_id, quantity)");

        // Random data generator
        Random random = new Random();

        // Prepopulate the tables in preparation for evaluation
        prepopulateTables(random);

        // Loop to simulate millions of queries
        for (int i = 0; i < numberOfQueries; i++) {
            int queryType = random.nextInt(6);  // Randomly choose the type of query to execute

            switch (queryType) {
                case 0:  // INSERT query
                    insertRandomData(random);
                    break;
                case 1:  // SELECT query (simple)
                    selectRandomData(random);
                    break;
                case 2:  // UPDATE query
                    updateRandomData(random);
                    break;
                case 3:  // DELETE query
                    deleteRandomData(random);
                    break;
                case 4:  // Complex SELECT query with WHERE, AND, OR, >, <, LIKE
                    complexSelectQuery(random);
                    break;
                case 5:  // Complex UPDATE query with WHERE
                    complexUpdateQuery(random);
                    break;
            }

            // Print progress every 100,000 queries
            if (i % 10000 == 0){
                System.out.println("Processed " + i + " queries...");
            }
        }

        System.out.println("Finished processing " + numberOfQueries + " queries.");
    }

    private static void prepopulateTables(Random random) {
        System.out.println("Prepopulating users");
        // Insert initial users
        for (int i = 0; i < 50; i++) {
            String name = "User" + i;
            int age = 20 + (i % 41); // Ages between 20 and 60
            String city = getRandomCity(random);
            String insertCommand = String.format("INSERT INTO users VALUES (%d, '%s', %d, '%s')", i, name, age, city);
            selectedEngine.executeSQL(insertCommand);
        }
        System.out.println("Prepopulating products");
        // Insert initial products
        for (int i = 0; i < 50; i++) {
            String productName = "Product" + i;
            double price = 10 + (i % 990); // Prices between $10 and $1000
            String category = getRandomCategory(random);
            String insertCommand = String.format("INSERT INTO products VALUES (%d, '%s', %.2f, '%s')", i, productName, price, category);
            selectedEngine.executeSQL(insertCommand);
        }
        System.out.println("Prepopulating orders");
        // Insert initial orders
        for (int i = 0; i < 50; i++) {
            int user_id = random.nextInt(9999);
            int product_id = random.nextInt(9999);
            int quantity = random.nextInt(1, 100);
            String insertCommand = String.format("INSERT INTO orders VALUES (%d, %d, %d, %d)", i, user_id, product_id, quantity);
            selectedEngine.executeSQL(insertCommand);
        }
    }

    // Helper method to insert random data into users, products, or orders table
    private static void insertRandomData(Random random) {
        int tableChoice = random.nextInt(3);
        switch (tableChoice) {
            case 0: // Insert into users table
                int id = random.nextInt(10000) + 10000;
                String name = "User" + id;
                int age = random.nextInt(60) + 20;
                String city = getRandomCity(random);
                String insertUserQuery = "INSERT INTO users VALUES (" + id + ", '" + name + "', " + age + ", '" + city + "')";
                selectedEngine.executeSQL(insertUserQuery);
                break;
            case 1: // Insert into products table
                int productId = random.nextInt(1000) + 10000;
                String productName = "Product" + productId;
                double price = 50 + (random.nextDouble() * 1000);
                String category = getRandomCategory(random);
                String insertProductQuery = "INSERT INTO products VALUES (" + productId + ", '" + productName + "', " + price + ", '" + category + "')";
                selectedEngine.executeSQL(insertProductQuery);
                break;
            case 2: // Insert into orders table
                int orderId = random.nextInt(10000) + 1;
                int userId = random.nextInt(10000) + 1;
                int productIdRef = random.nextInt(1000) + 1;
                int quantity = random.nextInt(10) + 1;
                String insertOrderQuery = "INSERT INTO orders VALUES (" + orderId + ", " + userId + ", " + productIdRef + ", " + quantity + ")";
                selectedEngine.executeSQL(insertOrderQuery);
                break;
        }
    }

    // Helper method to randomly select data from tables
    private static void selectRandomData(Random random) {
        int tableChoice = random.nextInt(3);
        String selectQuery;
        switch (tableChoice) {
            case 0:
                selectQuery = "SELECT * FROM users";
                break;
            case 1:
                selectQuery = "SELECT * FROM products";
                break;
            case 2:
                selectQuery = "SELECT * FROM orders";
                break;
            default:
                selectQuery = "SELECT * FROM users";
        }
        selectedEngine.executeSQL(selectQuery);
    }

    // Helper method to update random data in the tables
    private static void updateRandomData(Random random) {
        int tableChoice = random.nextInt(3);
        switch (tableChoice) {
            case 0: // Update users table
                int id = random.nextInt(10000) + 1;
                int newAge = random.nextInt(60) + 20;
                String updateUserQuery = "UPDATE users SET age = " + newAge + " WHERE id = " + id;
                selectedEngine.executeSQL(updateUserQuery);
                break;
            case 1: // Update products table
                int productId = random.nextInt(1000) + 1;
                double newPrice = 50 + (random.nextDouble() * 1000);
                String updateProductQuery = "UPDATE products SET price = " + newPrice + " WHERE id = " + productId;
                selectedEngine.executeSQL(updateProductQuery);
                break;
            case 2: // Update orders table
                int orderId = random.nextInt(10000) + 1;
                int newQuantity = random.nextInt(10) + 1;
                String updateOrderQuery = "UPDATE orders SET quantity = " + newQuantity + " WHERE id = " + orderId;
                selectedEngine.executeSQL(updateOrderQuery);
                break;
        }
    }

    // Helper method to delete random data from tables
    private static void deleteRandomData(Random random) {
        int tableChoice = random.nextInt(3);
        switch (tableChoice) {
            case 0: // Delete from users table
                int userId = random.nextInt(10000) + 1;
                String deleteUserQuery = "DELETE FROM users WHERE id = " + userId;
                selectedEngine.executeSQL(deleteUserQuery);
                break;
            case 1: // Delete from products table
                int productId = random.nextInt(1000) + 1;
                String deleteProductQuery = "DELETE FROM products WHERE id = " + productId;
                selectedEngine.executeSQL(deleteProductQuery);
                break;
            case 2: // Delete from orders table
                int orderId = random.nextInt(10000) + 1;
                String deleteOrderQuery = "DELETE FROM orders WHERE id = " + orderId;
                selectedEngine.executeSQL(deleteOrderQuery);
                break;
        }
    }

    // Helper method to execute a complex SELECT query with WHERE, AND, OR, >, <, LIKE
    private static void complexSelectQuery(Random random) {
        int tableChoice = random.nextInt(2);  // Complex queries only on users and products for now
        String complexSelectQuery;
        switch (tableChoice) {
            case 0: // Complex SELECT on users
                int minAge = random.nextInt(20) + 20;
                int maxAge = minAge + random.nextInt(30);
                String city = getRandomCity(random);
                complexSelectQuery = "SELECT * FROM users WHERE age > " + minAge + " AND age < " + maxAge;
                break;
            case 1: // Complex SELECT on products
                double minPrice = 50 + (random.nextDouble() * 200);
                double maxPrice = minPrice + random.nextDouble() * 500;
                complexSelectQuery = "SELECT * FROM products WHERE price > " + minPrice + " AND price < " + maxPrice;
                break;
            case 2: // Complex SELECT on products
                double minPrice2 = 50 + (random.nextDouble() * 200);
                String category = getRandomCategory(random);
                complexSelectQuery = "SELECT * FROM products WHERE price > " + minPrice2 + " AND category = " + category;
                break;
            default:
                complexSelectQuery = "SELECT * FROM users";
        }
        selectedEngine.executeSQL(complexSelectQuery);
    }

    // Helper method to execute a complex UPDATE query with WHERE
    private static void complexUpdateQuery(Random random) {
        int tableChoice = random.nextInt(2);  // Complex updates only on users and products for now
        switch (tableChoice) {
            case 0: // Complex UPDATE on users
                int newAge = random.nextInt(60) + 20;
                String city = getRandomCity(random);
                String updateUserQuery = "UPDATE users SET age = " + newAge + " WHERE city = '" + city + "'";
                selectedEngine.executeSQL(updateUserQuery);
                break;
            case 1: // Complex UPDATE on products
                double newPrice = 50 + (random.nextDouble() * 1000);
                String category = getRandomCategory(random);
                String updateProductQuery = "UPDATE products SET price = " + newPrice + " WHERE category = '" + category + "'";
                selectedEngine.executeSQL(updateProductQuery);
                break;
        }
    }

    // Helper method to return a random city
    private static String getRandomCity(Random random) {
        String[] cities = {"New York", "Los Angeles", "Chicago", "Boston", "Miami", "Seattle", "Austin", "Dallas", "Atlanta", "Denver"};
        return cities[random.nextInt(cities.length)];
    }

    // Helper method to return a random category for products
    private static String getRandomCategory(Random random) {
        String[] categories = {"Electronics", "Appliances", "Clothing", "Furniture", "Toys", "Sports", "Books", "Beauty", "Garden"};
        return categories[random.nextInt(categories.length)];
    }
}