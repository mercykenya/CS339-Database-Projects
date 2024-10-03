# CS 339 Database Systems Projects

This repository contains the four projects from my CS 339 (Introduction to Database Systems) course. Each project focuses on a different aspect of databases, ranging from writing SQL queries to building components of a database management system (DBMS).

## Project 1: Writing SQL Queries
In this project, I wrote SQL queries against a real database to gain hands-on experience interacting with relational databases. The queries focused on data retrieval, filtering, aggregation, and updating information within the database. This project reinforced fundamental SQL concepts such as `SELECT`, `JOIN`, `GROUP BY`, and `ORDER BY` while allowing me to practice query optimization techniques for efficiency.

## Project 2: Building a Storage Engine in SimpleDB
This project provided an overview of the architecture of a Database Management System (DBMS) by focusing on the implementation of a storage engine. I built the storage layer for SimpleDB, a lightweight database developed by MIT. The storage engine handles reading and writing data to disk, and the assignment gave me an in-depth understanding of file management, buffer management, and how databases persist data in an efficient and structured manner.

## Project 3: Implementing Database Operators
In this project, I implemented various database operators, such as `Selection`, `Projection`, and `Join`, which form the building blocks for executing SQL queries in SimpleDB. These operators are designed to be modular and composable, meaning they can be combined in different ways to process SQL queries. This project emphasized understanding query execution plans, how these operators work together, and the efficiency trade-offs involved in query execution.

## Project 4: Query Optimizer for SimpleDB
This project focused on designing and implementing a query optimizer for SimpleDB. The query optimizer estimates the cost of different query execution plans and selects the most efficient one. This involved using statistics like table size and data distribution to estimate query execution costs and building a cost-based optimization framework. The project gave me a deeper understanding of how modern databases optimize queries behind the scenes to ensure fast and efficient data retrieval.
