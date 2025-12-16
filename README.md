# Database Engine with B+ Tree Indexing 🚀

![Java](https://img.shields.io/badge/Language-Java-blue) ![License](https://img.shields.io/badge/License-MIT-green) ![Build](https://img.shields.io/badge/Build-Maven-yellow)

A **lightweight Java-based database engine** supporting table management, CRUD operations, and **B+ tree indexing**. Ideal for learning core database concepts, memory-efficient storage, and query optimization.


---

## Table of Contents

* [Overview](#overview)
* [Features](#features)
* [Architecture](#architecture)
* [Example Usage](#example-usage)
* [Directory Structure](#directory-structure)
* [Technology Stack](#technology-stack)
* [Future Enhancements](#future-enhancements)
* [References](#references)

---

## Overview

This project implements a **simplified relational database engine** in Java.

Key functionalities:

* Create tables with typed columns (`Integer`, `String`, `Double`)
* Insert, update, and delete records efficiently
* Perform linear searches or use **B+ tree indices** for fast queries
* Persist tables and indices to disk

> The project focuses on **core database concepts** like page-based storage, indexing, metadata management, and memory optimization.

---

## Features

<details>
<summary>Tables & Pages</summary>

* Tables are stored as **binary pages** on disk.
* Each page holds a fixed number of rows (configurable, e.g., 200).
* **Lazy loading**: pages are loaded into memory only when needed.
* Tuples are stored as **serialized `Vector` objects**.
* Each page and tuple implements `toString()` for inspection.

</details>

<details>
<summary>Metadata Management</summary>

* Maintains **metadata.csv** tracking table structure, column types, clustering keys, and indices.
* Validates column types during all operations.
* Ensures consistent use of indices during queries.

</details>

<details>
<summary>Indexing</summary>

* Supports **B+ tree indices** for fast query execution.
* Indices can be created dynamically and updated automatically.
* Indices are persisted to disk and loaded on application startup.

</details>

<details>
<summary>Querying</summary>

* Queries executed via object-oriented API (no SQL parsing required).
* Supports **complex conditions**: `AND`, `OR`, `XOR`.
* Returns results as an `Iterator` for memory-efficient traversal.

</details>

---

## Architecture

<details>
<summary>Core Classes</summary>

* **DBApp.java** – main class providing API for table creation, insertion, update, deletion, indexing, and querying
* **Page** – represents a single page on disk containing serialized tuples
* **Table** – stores page filenames and schema metadata
* **SQLTerm** – represents a query condition
* **Iterator** – traverses query results efficiently

</details>

<details>
<summary>Data Storage & Indexing</summary>

* Tables split into multiple pages to avoid full memory loading
* Tuples serialized inside pages
* Metadata ensures type safety and tracks which columns have indices
* B+ tree indices accelerate equality and range queries
* Indexes updated on insertion/deletion

</details>

---

## Example Usage

```java
DBApp dbApp = new DBApp();
Hashtable<String,String> colTypes = new Hashtable<>();
colTypes.put("id", "java.lang.Integer");
colTypes.put("name", "java.lang.String");
colTypes.put("gpa", "java.lang.Double");

dbApp.createTable("Student", "id", colTypes);
dbApp.createIndex("Student", "gpa", "GPAIndex");

Hashtable<String,Object> row = new Hashtable<>();
row.put("id", 2343432);
row.put("name", "Ahmed Noor");
row.put("gpa", 0.95);
dbApp.insertIntoTable("Student", row);

SQLTerm[] terms = new SQLTerm[2];
terms[0] = new SQLTerm("Student", "name", "=", "John Noor");
terms[1] = new SQLTerm("Student", "gpa", "=", 1.5);
String[] operators = {"OR"};

Iterator resultSet = dbApp.selectFromTable(terms, operators);
```

---

## Directory Structure

```
App
├───src
│   ├───main
│   │   └───java
│   │       └───teamdb
│   │           ├───classes
│   │           ├───resources
│   │           └───Serializations
│   └───test
│       └───java
│           └───teamdb
└───target
    ├───classes
    │   └───teamdb
    │       ├───classes
    │       └───resources
    └───test-classes
        └───teamdb
```
## File Abstract Explanation

### Root of `teamdb` folder:

* `App.java` → Main application entry point or helper app.
* `DBApp.java` → The core database application class.
* `metadata.csv` → Stores table/column metadata for your database engine.
* `Run.java` → Runner file.
* `SQLTerm.java` → Represent an SQL query condition or expression.

### `classes` folder:

* `bplustree.java` / `BTree.java` → Implementation of B+ Tree or B-Tree for indexing.
* `ConfigReader.java` → Reads configuration files.
* `DBAppException.java` → Custom exception class for database errors.
* `Helpers.java` → Utility methods.
* `Index.java` → Logic for handling indexes.
* `Main.java` → Main file.
* `Notes.txt` → Some notes about metadata.
* `Page.java` / `PageIndex.java` → Classes representing disk pages and page indexing.
* `Pair.java` → A simple key-value pair class.
* `Serializer.java` → Handles serializing objects to disk.
* `Table.java` → Core table representation.
* `Tuple.java` → Represents a row in a table.

---

## Technology Stack

* **Language:** Java
* **Libraries:** Optional B+ tree implementations
* **IDE:** IntelliJ, Eclipse, NetBeans
* **Build Tools:** Maven, Ant, Make

---

## Future Enhancements

* Full SQL parsing using **ANTLR**
* Support for joins and foreign keys
* Transaction management and ACID compliance

---

## References

* [Java Serialization Tutorial](https://www.tutorialspoint.com/java/java_serialization.htm)
* [Java Reflection API](http://download.oracle.com/javase/tutorial/reflect/)
* [B+ Tree Library](https://github.com/shandysulen/B-Plus-Tree): Forked and implemented some modifications to achieve what was needed.

