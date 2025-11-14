"""
SQL Query to analyze customer spending and order statistics.
Joins customers, orders, order_items, and products tables.
"""

import sqlite3
import pandas as pd
from tabulate import tabulate

# Database name
DB_NAME = 'ecommerce.db'

def execute_customer_analysis():
    """Execute SQL query to analyze customer spending and display results."""
    
    # SQL Query
    query = """
    SELECT 
        c.name AS customer_name,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(oi.quantity) AS total_items_purchased,
        ROUND(SUM(oi.quantity * oi.price), 2) AS total_amount_spent
    FROM 
        customers c
    INNER JOIN 
        orders o ON c.customer_id = o.customer_id
    INNER JOIN 
        order_items oi ON o.order_id = oi.order_id
    INNER JOIN 
        products p ON oi.product_id = p.product_id
    GROUP BY 
        c.customer_id, c.name
    HAVING 
        COUNT(DISTINCT o.order_id) >= 1
    ORDER BY 
        total_amount_spent DESC;
    """
    
    try:
        # Connect to database
        conn = sqlite3.connect(DB_NAME)
        
        # Execute query and load into pandas DataFrame
        print("Executing customer analysis query...")
        df = pd.read_sql_query(query, conn)
        
        # Close connection
        conn.close()
        
        # Display results
        print("\n" + "="*80)
        print("CUSTOMER SPENDING ANALYSIS")
        print("="*80)
        print(f"\nTotal customers with orders: {len(df)}")
        print("\nResults (ordered by total amount spent):")
        print("-"*80)
        
        # Format the DataFrame for better display
        df_display = df.copy()
        df_display['total_amount_spent'] = df_display['total_amount_spent'].apply(
            lambda x: f"${x:,.2f}"
        )
        
        # Display using tabulate for nice formatting
        print(tabulate(
            df_display,
            headers=['Customer Name', 'Total Orders', 'Total Items', 'Total Amount Spent'],
            tablefmt='grid',
            showindex=False
        ))
        
        # Summary statistics
        print("\n" + "-"*80)
        print("SUMMARY STATISTICS")
        print("-"*80)
        print(f"Average amount spent per customer: ${df['total_amount_spent'].mean():,.2f}")
        print(f"Median amount spent per customer: ${df['total_amount_spent'].median():,.2f}")
        print(f"Highest spending customer: {df.iloc[0]['customer_name']} (${df.iloc[0]['total_amount_spent']:,.2f})")
        print(f"Total revenue from all customers: ${df['total_amount_spent'].sum():,.2f}")
        print(f"Average orders per customer: {df['total_orders'].mean():.2f}")
        print(f"Average items per customer: {df['total_items_purchased'].mean():.2f}")
        print("="*80)
        
        return df
        
    except sqlite3.Error as e:
        print(f"✗ Database error: {e}")
        return None
    except FileNotFoundError:
        print(f"✗ Error: Database file '{DB_NAME}' not found!")
        print("   Please run 'create_database.py' first to create the database.")
        return None
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return None

def display_query_explanation():
    """Display explanation of the SQL query."""
    print("\n" + "="*80)
    print("SQL QUERY EXPLANATION")
    print("="*80)
    print("""
The query performs the following operations:

1. JOINS:
   - customers → orders (via customer_id)
   - orders → order_items (via order_id)
   - order_items → products (via product_id)

2. AGGREGATIONS:
   - COUNT(DISTINCT o.order_id): Total unique orders per customer
   - SUM(oi.quantity): Total items purchased across all orders
   - SUM(oi.quantity * oi.price): Total amount spent (revenue per customer)

3. FILTERING:
   - INNER JOIN ensures only customers with orders are included
   - HAVING clause explicitly filters customers with at least 1 order

4. ORDERING:
   - Results sorted by total_amount_spent in descending order
   - Shows top spenders first
    """)
    print("="*80)

if __name__ == "__main__":
    display_query_explanation()
    execute_customer_analysis()

