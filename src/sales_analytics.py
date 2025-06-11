"""
Sales Analytics

This module analyzes sales data and extracts customer purchase patterns
for marketing strategy targeting the 18–35 age group.

Author: Hemanth Aradhya B R [aradhyahemanth31@gmail.com]
Date: June 2025
"""

# Importing required libraries
import sqlite3  # For connecting to SQLite database
import pandas as pd  # For data manipulation and analysis
from typing import List, Tuple  # For type hinting


# Define a class to encapsulate the entire analytics logic
class SalesAnalytics:
    def __init__(self, db_path: str):
        # Initialize with the path to the database
        self.db_path = db_path
        self.connection = None  # Placeholder for DB connection

    def connect_to_database(self) -> bool:
        # Try to connect to the SQLite database
        try:
            self.connection = sqlite3.connect(self.db_path)
            print(f"Successfully connected to database: {self.db_path}")
            return True
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")
            return False

    def close_connection(self):
        # Close the connection if it's open
        if self.connection:
            self.connection.close()
            print("Database connection closed.")

    def extract_data_sql_approach(self) -> List[Tuple]:
        # SQL query to extract age-18–35 customer purchase patterns
        sql_query = """
        SELECT 
            c.customer_id,
            c.age,
            i.item_name,
            CAST(SUM(o.quantity) AS INTEGER) as total_quantity
        FROM Customer c
        INNER JOIN Sales s ON c.customer_id = s.customer_id
        INNER JOIN Orders o ON s.sales_id = o.sales_id
        INNER JOIN Items i ON o.item_id = i.item_id
        WHERE c.age BETWEEN 18 AND 35
        AND o.quantity IS NOT NULL
        AND o.quantity > 0
        GROUP BY c.customer_id, c.age, i.item_name
        HAVING SUM(o.quantity) > 0
        ORDER BY c.customer_id, i.item_name;
        """

        # Execute query and fetch results
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql_query)
            results = cursor.fetchall()
            print(f"SQL Approach: Retrieved {len(results)} records")
            return results
        except sqlite3.Error as e:
            print(f"Error executing SQL query: {e}")
            return []

    def extract_data_pandas_approach(self) -> pd.DataFrame:
        # Pandas-based approach to extract and analyze customer purchases
        try:
            # Load all tables into pandas DataFrames
            customers_df = pd.read_sql_query("SELECT * FROM Customer", self.connection)
            sales_df = pd.read_sql_query("SELECT * FROM Sales", self.connection)
            orders_df = pd.read_sql_query("SELECT * FROM Orders", self.connection)
            items_df = pd.read_sql_query("SELECT * FROM Items", self.connection)

            # Filter customers aged 18 to 35
            customers_filtered = customers_df[
                (customers_df['age'] >= 18) & (customers_df['age'] <= 35)
            ]

            # Merge customer → sales → orders → items
            merged_df = (customers_filtered
                         .merge(sales_df, on='customer_id')
                         .merge(orders_df, on='sales_id')
                         .merge(items_df, on='item_id'))

            # Filter rows where quantity is non-null and positive
            merged_df = merged_df[
                (merged_df['quantity'].notna()) & (merged_df['quantity'] > 0)
            ]

            # Group by customer, age, and item, then sum quantity
            result_df = (merged_df
                         .groupby(['customer_id', 'age', 'item_name'])['quantity']
                         .sum()
                         .reset_index())

            # Ensure quantity is an integer
            result_df['quantity'] = result_df['quantity'].astype(int)

            # Optional safety check: remove zero quantities
            result_df = result_df[result_df['quantity'] > 0]

            # Sort results for consistency
            result_df = result_df.sort_values(['customer_id', 'item_name']).reset_index(drop=True)

            print(f"Pandas Approach: Retrieved {len(result_df)} records")
            return result_df

        except Exception as e:
            print(f"Error in pandas approach: {e}")
            return pd.DataFrame()

    def save_to_csv(self, data, filename: str, approach: str = "sql"):
        # Save results to CSV file with semicolon (;) as delimiter
        try:
            if approach == "sql":
                # Convert list of tuples to DataFrame
                df = pd.DataFrame(data, columns=['Customer', 'Age', 'Item', 'Quantity'])
            else:
                # Rename columns for consistency
                df = data.copy()
                df.columns = ['Customer', 'Age', 'Item', 'Quantity']

            # Save to CSV
            df.to_csv(filename, sep=';', index=False)
            print(f"Data successfully saved to {filename}")

            # Display data preview
            print("\nPreview of saved data:")
            print(df.to_string(index=False))

        except Exception as e:
            print(f"Error saving to CSV: {e}")

    def analyze_sales_data(self, output_filename: str = "sales_analysis.csv", output_dir: str = "output"):
        # Entry point for analyzing and exporting both approaches
        print("=" * 60)
        print("SALES DATA ANALYSIS")
        print("=" * 60)

        # --- SQL-based extraction ---
        print("\nSOLUTION 1: Pure SQL Approach")
        print("-" * 40)
        sql_results = self.extract_data_sql_approach()
        if sql_results:
            sql_filename = f"{output_dir}/sql_{output_filename}"
            self.save_to_csv(sql_results, sql_filename, "sql")

        # --- Pandas-based extraction ---
        print("\nSOLUTION 2: Pandas Approach")
        print("-" * 40)
        pandas_results = self.extract_data_pandas_approach()
        if not pandas_results.empty:
            pandas_filename = f"{output_dir}/pandas_{output_filename}"
            self.save_to_csv(pandas_results, pandas_filename, "pandas")

        # --- Verification of consistency ---
        if sql_results and not pandas_results.empty:
            self._verify_results(sql_results, pandas_results)

        return sql_results, pandas_results

    def _verify_results(self, sql_results: List[Tuple], pandas_results: pd.DataFrame):
        # Compares SQL and Pandas results to ensure they match
        print("\nVERIFICATION: Comparing Results")
        print("-" * 40)

        # Convert both to DataFrames
        sql_df = pd.DataFrame(sql_results, columns=['Customer', 'Age', 'Item', 'Quantity'])
        pandas_df = pandas_results.copy()
        pandas_df.columns = ['Customer', 'Age', 'Item', 'Quantity']

        # Sort for row-by-row comparison
        sql_df = sql_df.sort_values(['Customer', 'Item']).reset_index(drop=True)
        pandas_df = pandas_df.sort_values(['Customer', 'Item']).reset_index(drop=True)

        # Compare for equality
        if sql_df.equals(pandas_df):
            print("Both approaches produce identical results!")
        else:
            print("Results differ between approaches. This needs investigation.")


# Main script execution starts here
def main():
    # Set the path to your SQLite database file
    db_path = "D:/sales-analytics/data/Data Engineer_ETL Assignment 1.db"

    # Define the directory where output CSVs will be saved
    output_path = "D:/sales-analytics/output"

    # Create an instance of the SalesAnalytics class
    analytics = SalesAnalytics(db_path)

    try:
        # Connect to database
        if not analytics.connect_to_database():
            print("Failed to connect to database. Please check the database path.")
            return

        # Run the full analysis
        analytics.analyze_sales_data("sales_analysis.csv", output_dir=output_path)

        # Final confirmation
        print("\n" + "=" * 60)
        print("ANALYSIS COMPLETE")
        print("=" * 60)
        print("Generated files:")
        print("• output/sql_sales_analysis.csv")
        print("• output/pandas_sales_analysis.csv")

    except Exception as e:
        print(f"An error occurred during analysis: {e}")

    finally:
        # Always close the database connection at the end
        analytics.close_connection()


# Ensures this runs only when the script is executed directly
if __name__ == "__main__":
    main()
