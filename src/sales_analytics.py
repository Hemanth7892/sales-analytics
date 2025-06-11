"""
Sales Analytics

This module provides functionality to analyze sales data and extract customer
purchase patterns for marketing strategy targeting specific age groups[18-35].

Author: Hemanth Aradhya B R [aradhyahemanth31@gmail.com]
Date: June 2025
"""

import sqlite3
import pandas as pd
from typing import List, Tuple


class SalesAnalytics:
    """
    A class to analyze sales data and extract customer purchase patterns
    for marketing strategy targeting specific age groups.
    """

    def __init__(self, db_path: str):
        """
        Initialize the SalesAnalytics class with database connection.

        Args:
            db_path (str): Path to the SQLite database file
        """
        self.db_path = db_path
        self.connection = None

    def connect_to_database(self) -> bool:
        """
        Establish connection to the SQLite database.

        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.connection = sqlite3.connect(self.db_path)
            print(f"Successfully connected to database: {self.db_path}")
            return True
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")
            return False

    def close_connection(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            print("Database connection closed.")

    def extract_data_sql_approach(self) -> List[Tuple]:
        """
        Extract customer purchase data using pure SQL approach.
        Gets total quantities of each item bought per customer aged 18-35.

        Returns:
            List[Tuple]: List of tuples containing (customer_id, age, item_name, total_quantity)
        """
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
        """
        Extract customer purchase data using Pandas approach.
        Gets total quantities of each item bought per customer aged 18-35.

        Returns:
            pd.DataFrame: DataFrame containing customer purchase data
        """
        try:
            # Read all required tables into DataFrames
            customers_df = pd.read_sql_query("SELECT * FROM Customer", self.connection)
            sales_df = pd.read_sql_query("SELECT * FROM Sales", self.connection)
            orders_df = pd.read_sql_query("SELECT * FROM Orders", self.connection)
            items_df = pd.read_sql_query("SELECT * FROM Items", self.connection)

            # Filter customers aged 18-35
            customers_filtered = customers_df[
                (customers_df['age'] >= 18) & (customers_df['age'] <= 35)
                ]

            # Merge all tables step by step
            merged_df = (customers_filtered
                         .merge(sales_df, on='customer_id')
                         .merge(orders_df, on='sales_id')
                         .merge(items_df, on='item_id'))

            # Filter out NULL quantities and zero quantities
            merged_df = merged_df[
                (merged_df['quantity'].notna()) & (merged_df['quantity'] > 0)
                ]

            # Group by customer, age, and item to get total quantities
            result_df = (merged_df
                         .groupby(['customer_id', 'age', 'item_name'])['quantity']
                         .sum()
                         .reset_index())

            # Convert quantity to integer (no decimal points)
            result_df['quantity'] = result_df['quantity'].astype(int)

            # Filter out zero total quantities (safety check)
            result_df = result_df[result_df['quantity'] > 0]

            # Sort by customer_id and item_name for consistent output
            result_df = result_df.sort_values(['customer_id', 'item_name']).reset_index(drop=True)

            print(f"Pandas Approach: Retrieved {len(result_df)} records")
            return result_df

        except Exception as e:
            print(f"Error in pandas approach: {e}")
            return pd.DataFrame()

    def save_to_csv(self, data, filename: str, approach: str = "sql"):
        """
        Save the extracted data to CSV file with semicolon delimiter.

        Args:
            data: Either list of tuples (SQL approach) or DataFrame (Pandas approach)
            filename (str): Output CSV filename
            approach (str): Either "sql" or "pandas" to determine data format
        """
        try:
            if approach == "sql":
                # Convert SQL results to DataFrame for consistent CSV output
                df = pd.DataFrame(data, columns=['Customer', 'Age', 'Item', 'Quantity'])
            else:
                # Rename columns to match expected output format
                df = data.copy()
                df.columns = ['Customer', 'Age', 'Item', 'Quantity']

            # Save to CSV with semicolon delimiter
            df.to_csv(filename, sep=';', index=False)
            print(f"Data successfully saved to {filename}")

            # Print preview of the saved data
            print("\nPreview of saved data:")
            print(df.to_string(index=False))

        except Exception as e:
            print(f"Error saving to CSV: {e}")

    def analyze_sales_data(self, output_filename: str = "sales_analysis.csv"):
        """
        Main method to analyze sales data using both approaches and save results.

        Args:
            output_filename (str): Name of the output CSV file
        """
        # update your output path
        output_dir = "D:/sales-analytics/output"

        print("=" * 60)
        print("SALES DATA ANALYSIS")
        print("=" * 60)

        # Solution 1: SQL Approach
        print("\nSOLUTION 1: Pure SQL Approach")
        print("-" * 40)
        sql_results = self.extract_data_sql_approach()

        if sql_results:
            sql_filename = f"{output_dir}/sql_{output_filename}"
            self.save_to_csv(sql_results, sql_filename, "sql")

        # Solution 2: Pandas Approach
        print("\nSOLUTION 2: Pandas Approach")
        print("-" * 40)
        pandas_results = self.extract_data_pandas_approach()

        if not pandas_results.empty:
            pandas_filename = f"{output_dir}/pandas_{output_filename}"
            self.save_to_csv(pandas_results, pandas_filename, "pandas")

        # Verification - compare results
        if sql_results and not pandas_results.empty:
            self._verify_results(sql_results, pandas_results)

        return sql_results, pandas_results

    def _verify_results(self, sql_results: List[Tuple], pandas_results: pd.DataFrame):
        """
        Verify that both approaches produce identical results.

        Args:
            sql_results: Results from SQL approach
            pandas_results: Results from Pandas approach
        """
        print("\nVERIFICATION: Comparing Results")
        print("-" * 40)

        # Convert SQL results to DataFrame for comparison
        sql_df = pd.DataFrame(sql_results, columns=['Customer', 'Age', 'Item', 'Quantity'])
        pandas_df = pandas_results.copy()
        pandas_df.columns = ['Customer', 'Age', 'Item', 'Quantity']

        # Sort both for comparison
        sql_df = sql_df.sort_values(['Customer', 'Item']).reset_index(drop=True)
        pandas_df = pandas_df.sort_values(['Customer', 'Item']).reset_index(drop=True)

        if sql_df.equals(pandas_df):
            print("Both approaches produce identical results!")
        else:
            print("Results differ between approaches")
            print("This needs investigation.")


def main():
    """
    Main function to run the sales analytics.
    Usage: Modify the db_path variable to point to your SQLite database file.
    """
    # Path to the SQLite database file
    db_path = "D:/sales-analytics/data/Data Engineer_ETL Assignment 1.db"

    # Initialize the analytics class
    analytics = SalesAnalytics(db_path)

    try:
        # Connect to database
        if not analytics.connect_to_database():
            print("Failed to connect to database. Please check the database path.")
            return

        # Run the analysis
        analytics.analyze_sales_data("sales_analysis.csv")

        print("\n" + "=" * 60)
        print("ANALYSIS COMPLETE")
        print("=" * 60)
        print("Generated files:")
        print("• sql_sales_analysis.csv - Results from SQL approach")
        print("• pandas_sales_analysis.csv - Results from Pandas approach")

    except Exception as e:
        print(f"An error occurred during analysis: {e}")

    finally:
        # Always close the database connection
        analytics.close_connection()


if __name__ == "__main__":
    main()
