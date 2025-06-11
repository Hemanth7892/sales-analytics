# 📊 Sales Analytics – Purchase Pattern Extraction

This project analyzes sales transaction data to identify **purchase patterns of customers aged 18–35**, enabling targeted marketing strategies.

---

## 📁 Project Structure

```
sales-analytics/
│
├── data/
│   └── Data Engineer_ETL Assignment 1.db   # SQLite DB file
│
├── output/
│   ├── sql_sales_analysis.csv              # SQL results
│   └── pandas_sales_analysis.csv           # Pandas results
│
├── sales_analytics.py                      # Main analysis script
└── README.md                               # Project documentation
```

---

## ⚙️ Requirements

Install the required packages:

```bash
pip install pandas
```

Python version: **3.1+ recommended**

---

## 🚀 How to Run

### Step 1: Update your paths

In `main()` function of `sales_analytics.py`, set:

```python
db_path = "data/Data Engineer_ETL Assignment 1.db"
output_path = "output"
```

### Step 2: Run the script

```bash
python sales_analytics.py
```

### ✅ Output

After successful run, two CSV files will be generated inside the `output/` folder:

- `sql_sales_analysis.csv` – Extracted using raw SQL queries
- `pandas_sales_analysis.csv` – Extracted using Pandas transformations

---

## 📌 Script Usage Summary

```python
analytics = SalesAnalytics(db_path)
analytics.connect_to_database()
analytics.analyze_sales_data("sales_analysis.csv", output_dir="output")
analytics.close_connection()
```

You can import the class and call methods individually for reuse in larger pipelines or notebooks.

---

## 🧠 Logic Details

### 🔍 SQL Approach

- Performs a multi-table join:
  - `Customer` → `Sales` → `Orders` → `Items`
- Filters only customers **aged between 18 and 35**
- Aggregates total quantity per `(customer_id, item_name)`
- Removes zero/null quantities
- Final data sorted by `customer_id` and `item_name`

**Advantages:** Fast, native to the database, minimal RAM usage  
**Drawbacks:** Less flexible for dynamic transformations

---

### 📊 Pandas Approach

- Loads all 4 tables into Pandas DataFrames
- Applies age filter (`18 <= age <= 35`)
- Performs inner joins using `.merge()`
- Filters out:
  - null quantities
  - zero quantities
- Groups by `(customer_id, item_name)` and **sums the quantities**
- Cleans and sorts the final DataFrame before exporting


---

## ✅ Verification

Both approaches are compared at the end of the script using:

```python
if sql_df.equals(pandas_df):
    print("Both approaches produce identical results!")
```

This ensures the **Pandas and SQL outputs are consistent** and reliable.

---

## 👨‍💻 Author

**Hemanth Aradhya B R**  
📧 aradhyahemanth31@gmail.com

---

