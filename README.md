Sales Analytics Assignment

A Python application that analyzes sales data to extract customer purchase patterns for marketing strategy targeting specific age groups (18-35).

Project Overview

Company XYZ held a promotional sale for their signature items (x, y, z) and wants to create a marketing strategy by analyzing total quantities purchased by different age groups.

Business Requirements
- Extract total quantities of each item bought per customer aged 18-35
- Aggregate quantities across multiple transactions per customer
- Exclude items with no purchases (NULL or zero quantities)
- Output data without decimal points (whole numbers only)

Database Schema

The application works with a SQLite database containing these tables:

- Customer: customer_id (PK), age
- Sales: sales_id (PK), customer_id (FK)
- Orders: order_id (PK), sales_id (FK), item_id (FK), quantity
- Items: item_id (PK), item_name

Features

- Dual Implementation: Both pure SQL and Pandas approaches
- Age Filtering: Targets customers aged 18-35 years
- Data Aggregation: Sums quantities across multiple transactions
- CSV Export: Semicolon-delimited output format
- Result Verification: Compares both approaches for accuracy

Project Structure

sales-analytics/
├── src/
│   └── sales_analytics.py     # Main application
├── data/
│   └──  Data Engineer_ETL Assignment 1.db      # Placeholder for database files
├── output/
│   └──           # Generated CSV files location
├── README.md


Usage

Basic Usage

from src.sales_analytics import SalesAnalytics

# Initialize with your database path
analytics = SalesAnalytics("data/Data Engineer_ETL Assignment 1.db")

# Update yout ouptput path in output_dir

# Connect to database
if analytics.connect_to_database():
    # Run analysis
    analytics.analyze_sales_data("sales_results.csv")
    analytics.close_connection()

Command Line Usage

python src/sales_analytics.py

Expected Output

The application generates CSV files with this format:

Customer;Age;Item;Quantity
1;21;x;10
2;23;x;1
2;23;y;1
2;23;z;1
3;35;z;2

Technical Implementation

Solution 1: Pure SQL Approach
- Uses SQL JOIN operations across all tables
- Applies WHERE clause for age filtering
- Groups data using GROUP BY with SUM aggregation
- Filters out NULL/zero quantities with HAVING clause

Solution 2: Pandas Approach
- Reads tables into separate DataFrames
- Applies age filtering using boolean indexing
- Merges DataFrames using pandas merge operations
- Groups and aggregates using groupby() and sum()

Testing

Run the test suite:

python -m pytest tests/

Requirements

- Python 3.1+
- pandas
- sqlite3 (built-in)


Assignment Context


- SQL database interaction skills
- Data analysis and manipulation techniques
- Python programming best practices
- Business requirement analysis and implementation

