# Comparative Analysis of Relational and Non-Relational Databases | PostgreSQL vs. MongoDB

This project explores the use of PostgreSQL and MongoDB for analyzing Yelp review data. The goal was to compare the performance and suitability of these two database systems for different types of data analysis tasks.

## Dataset

The project used a subset of the [Yelp dataset]([url](https://www.yelp.com/dataset/documentation/main)), including information about businesses, reviews, and users. The dataset was truncated to reduce its size while preserving data integrity. The following modifications were made:

Rows in the Review table were removed where user_id and business_id did not exist in the User and Business tables.

The funny and cool attributes were removed from the Review table.

The friends attribute was removed from the User table.

The Tip and Checkin tables were excluded from the analysis.

## System Setup

The project used the following tools and libraries:

PostgreSQL: Version 17.2

psycopg2: Version 2.9.9

Python: Version 3.13.0

MongoDB: Version 6+

pymongo: Version 4.10.1

## PostgreSQL Setup

A local PostgreSQL database was set up using conda.

Data was transformed from JSON format and loaded using Python scripts and the psycopg2 library.

Foreign key constraints were implemented to ensure data integrity.

Database schema included Business, User, and Review tables.

## MongoDB Setup

Installed via Homebrew.

Data was loaded using a Python script with the pymongo library.

Data was filtered before insertion into collections.

## Tasks and Queries

The project implemented five analytical tasks and compared the performance of PostgreSQL and MongoDB in executing these tasks.

### Task 1: Multi-State Users with High Ratings

Problem: Find users who have written reviews for businesses in 3+ states and have an average review star rating of more than 3.5.

PostgreSQL: Used common table expressions (CTEs) with indexing, improving performance.

MongoDB: Used aggregation pipelines with $lookup, $group, and $match.

### Task 2: Top Helpful Reviewers by State

Problem: Find the 5 users with the highest average helpful votes per review for each state.

PostgreSQL: Used window functions (e.g., RANK()) and indexing.

MongoDB: Used multiple $lookup, $unwind, and array manipulations.

### Task 3: Most Common Review Words

Problem: Find the top 5 most common words in reviews, excluding stop words.

PostgreSQL: Used unnest and string_to_array functions.

MongoDB: Used aggregation pipelines with $split, $match, and $group.

### Task 4: States with Highest Takeout Proportion

Problem: Identify the top 5 states with the highest proportion of businesses offering "RestaurantsTakeOut".

PostgreSQL: Used CTEs and materialized views.

MongoDB: Used aggregation pipelines with optimized query structuring.

### Task 5: Review Count vs. Star Rating Correlation

Problem: Determine if the number of reviews correlates with average star rating.

PostgreSQL: Used window functions and conditional logic.

MongoDB: Not applicable due to lack of equivalent analytical functions.

## Performance and Tool Comparison

### PostgreSQL

#### Strengths:

Well-suited for structured data, complex joins, and aggregations.

Efficient analytics support with SQL functions such as CTEs and window functions.

#### Weaknesses:

More complex setup and schema maintenance.

Performance may degrade with large volumes of unstructured data.

### MongoDB

#### Strengths:

Flexible schema for unstructured data and dynamic schemas.

Suitable for handling high-volume semi-structured data.

#### Weaknesses:

Less powerful querying compared to PostgreSQL, especially for deep joins.

Debugging and optimizing queries can be challenging.

## Results and Findings

PostgreSQL: Demonstrated significant performance improvements with indexing and predicate pushdown for complex JOIN operations.

MongoDB: Improved insertion speeds through indexing and data sharding for high-volume non-relational data.

Recommendations: PostgreSQL is ideal for structured and relational data, while MongoDB excels in flexible, high-volume environments.
