# XQL: SQL Implementation with Multiple Data Structures

XQL is a unique SQL engine that allows users to execute SQL queries using various data structures as the underlying storage mechanism. This project demonstrates how different data structures, such as **B-Trees**, **TreeMap**, **HashMap**, **MapHeapMap**, **ArrayList**, and **CircularLinkedList**, impact the efficiency and performance of SQL operations. XQL also provides users with insights into the advantages and disadvantages of each data structure after executing a query.

## Features
- **SQL Execution on Multiple Data Structures**: Choose from various data structures to see how SQL operations like `SELECT`, `INSERT`, `UPDATE`, and `DELETE` behave.
- **Data Structure Insights**: After selecting a data structure, XQL shows the pros & cons of the data structure in carrying out SQL queries for reference. 
- **Customizable Engine**: The program is modular, allowing easy addition of new data structures in the future.

## Supported Data Structures
- **BTree - Rows**: A row-based self-balancing tree data structure optimized for searching, inserting, and deleting keys.
- **BTree - Columns**: A column-based self-balancing tree data structure optimized for searching, inserting, and deleting keys.
- **TreeMap**: A red-black tree-based implementation of a sorted map.
- **HashMap**: A hash-based implementation for fast key-value lookups.
- **MapHeapMap**: A custom implementation combining heap and map properties for efficient priority-based operations.
- **ArrayList**: A resizable array, offering fast indexed access and dynamic sizing.
- **CircularLinkedList**: A linked list where the last node points back to the first node, making traversal circular.

### Key Files and Directories
- **`CustomParser.java`**: Parses SQL queries into components for execution.
- **`EngineFactory.java`**: Factory class for creating engines with different data structures.
- **`Main.java`**: Entry point for the application.
- **Directories (e.g., `BTree`, `HashMap`)**: Contain implementations of specific data structures.

## Installation and Usage

### Prerequisites
- **Java Development Kit (JDK)** 17 or later.
- **Apache Maven** for build management.

### Steps to Run
1. Clone the repository:
   ```bash
   git clone https://github.com/mattw23n/XQL.git
   cd XQL
   ```
2. Compile the project using Maven:
   ```bash
   mvn compile
   ```
3. Run the program:
   ```bash
   mvn exec:java
   ```

## Example Commands
You can execute SQL-like commands directly using the engine. Below are examples:

### Create a Table
```sql
CREATE TABLE student (id, name, age, city);
```

### Insert Data
```sql
INSERT INTO student VALUES (1, John, 30, Florida);
INSERT INTO student VALUES (2, Jane, 25, Texas);
```

### Select All Data
```sql
SELECT * FROM student;
```

### Conditional Queries
```sql
SELECT * FROM student WHERE age > 20;
```

### Update Data
```sql
UPDATE student SET city = Houston WHERE name = John;
```

### Delete Data
```sql
DELETE FROM student WHERE city = Texas;
```


## Acknowledgments
- This project was developed to demonstrate the impact of different data structures on SQL operations.
- Built by SMU CS201 G2T1

---

Enjoy exploring SQL with XQL and uncovering the nuances of data structure performance!

