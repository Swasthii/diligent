"""
Create SQLite database and import CSV data into tables.
Reads customers, products, orders, order_items, and reviews CSV files.
"""

import sqlite3
import pandas as pd
import os
from datetime import datetime

# Database name
DB_NAME = 'ecommerce.db'

# CSV file names
CSV_FILES = {
    'customers': 'customers.csv',
    'products': 'products.csv',
    'orders': 'orders.csv',
    'order_items': 'order_items.csv',
    'reviews': 'reviews.csv'
}

def create_connection(db_name):
    """Create a database connection."""
    try:
        conn = sqlite3.connect(db_name)
        print(f"✓ Successfully connected to database: {db_name}")
        return conn
    except sqlite3.Error as e:
        print(f"✗ Error connecting to database: {e}")
        return None

def create_tables(conn):
    """Create all tables with proper schema."""
    cursor = conn.cursor()
    
    try:
        # Drop tables if they exist (for clean recreation)
        print("\nDropping existing tables if any...")
        tables = ['reviews', 'order_items', 'orders', 'products', 'customers']
        for table in tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        
        print("\nCreating tables...")
        
        # Customers table
        cursor.execute('''
            CREATE TABLE customers (
                customer_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                location TEXT,
                join_date DATE
            )
        ''')
        print("✓ Created table: customers")
        
        # Products table
        cursor.execute('''
            CREATE TABLE products (
                product_id INTEGER PRIMARY KEY,
                product_name TEXT NOT NULL,
                category TEXT,
                price REAL NOT NULL,
                stock_quantity INTEGER DEFAULT 0
            )
        ''')
        print("✓ Created table: products")
        
        # Orders table
        cursor.execute('''
            CREATE TABLE orders (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                order_date DATE,
                total_amount REAL NOT NULL,
                status TEXT,
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
            )
        ''')
        print("✓ Created table: orders")
        
        # Order items table
        cursor.execute('''
            CREATE TABLE order_items (
                order_item_id INTEGER PRIMARY KEY,
                order_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                price REAL NOT NULL,
                FOREIGN KEY (order_id) REFERENCES orders(order_id),
                FOREIGN KEY (product_id) REFERENCES products(product_id)
            )
        ''')
        print("✓ Created table: order_items")
        
        # Reviews table
        cursor.execute('''
            CREATE TABLE reviews (
                review_id INTEGER PRIMARY KEY,
                product_id INTEGER NOT NULL,
                customer_id INTEGER NOT NULL,
                rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 5),
                comment TEXT,
                review_date DATE,
                FOREIGN KEY (product_id) REFERENCES products(product_id),
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
            )
        ''')
        print("✓ Created table: reviews")
        
        conn.commit()
        print("\n✓ All tables created successfully!")
        return True
        
    except sqlite3.Error as e:
        print(f"✗ Error creating tables: {e}")
        conn.rollback()
        return False

def import_csv_to_table(conn, table_name, csv_file):
    """Import data from CSV file to SQLite table."""
    try:
        # Check if CSV file exists
        if not os.path.exists(csv_file):
            print(f"✗ Error: CSV file '{csv_file}' not found!")
            return False
        
        # Read CSV file
        print(f"\nReading {csv_file}...")
        df = pd.read_csv(csv_file)
        print(f"  Found {len(df)} records")
        
        # Insert data into table
        print(f"  Inserting data into '{table_name}' table...")
        df.to_sql(table_name, conn, if_exists='append', index=False)
        
        # Verify insertion
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        
        print(f"✓ Successfully inserted {count} records into '{table_name}' table")
        return True
        
    except pd.errors.EmptyDataError:
        print(f"✗ Error: CSV file '{csv_file}' is empty!")
        return False
    except pd.errors.ParserError as e:
        print(f"✗ Error parsing CSV file '{csv_file}': {e}")
        return False
    except sqlite3.Error as e:
        print(f"✗ Error inserting data into '{table_name}': {e}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error importing '{csv_file}': {e}")
        return False

def verify_data(conn):
    """Verify data integrity and print summary statistics."""
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("DATA VERIFICATION SUMMARY")
    print("="*60)
    
    try:
        tables = ['customers', 'products', 'orders', 'order_items', 'reviews']
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"{table.capitalize():20s}: {count:6d} records")
        
        # Additional statistics
        print("\n" + "-"*60)
        print("Additional Statistics:")
        print("-"*60)
        
        # Total revenue
        cursor.execute("SELECT SUM(total_amount) FROM orders")
        total_revenue = cursor.fetchone()[0]
        print(f"Total Revenue: ${total_revenue:,.2f}")
        
        # Average order value
        cursor.execute("SELECT AVG(total_amount) FROM orders")
        avg_order = cursor.fetchone()[0]
        print(f"Average Order Value: ${avg_order:,.2f}")
        
        # Total products sold
        cursor.execute("SELECT SUM(quantity) FROM order_items")
        total_sold = cursor.fetchone()[0]
        print(f"Total Products Sold: {total_sold:,}")
        
        # Average rating
        cursor.execute("SELECT AVG(rating) FROM reviews")
        avg_rating = cursor.fetchone()[0]
        if avg_rating:
            print(f"Average Product Rating: {avg_rating:.2f} stars")
        
        # Orders by status
        print("\nOrders by Status:")
        cursor.execute("""
            SELECT status, COUNT(*) as count 
            FROM orders 
            GROUP BY status 
            ORDER BY count DESC
        """)
        for row in cursor.fetchall():
            print(f"  {row[0]:15s}: {row[1]:4d}")
        
        print("="*60)
        
    except sqlite3.Error as e:
        print(f"✗ Error during verification: {e}")

def main():
    """Main function to orchestrate database creation and data import."""
    print("="*60)
    print("E-COMMERCE DATABASE CREATION")
    print("="*60)
    print(f"Database: {DB_NAME}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # Create database connection
    conn = create_connection(DB_NAME)
    if not conn:
        print("\n✗ Failed to create database connection. Exiting.")
        return
    
    try:
        # Create tables
        if not create_tables(conn):
            print("\n✗ Failed to create tables. Exiting.")
            return
        
        # Import data from CSV files
        print("\n" + "="*60)
        print("IMPORTING DATA FROM CSV FILES")
        print("="*60)
        
        success_count = 0
        for table_name, csv_file in CSV_FILES.items():
            if import_csv_to_table(conn, table_name, csv_file):
                success_count += 1
        
        # Verify all imports were successful
        if success_count == len(CSV_FILES):
            print("\n" + "="*60)
            print("✓ ALL DATA SUCCESSFULLY IMPORTED!")
            print("="*60)
            
            # Verify data
            verify_data(conn)
        else:
            print(f"\n⚠ Warning: Only {success_count} out of {len(CSV_FILES)} tables were imported successfully.")
        
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        conn.rollback()
    
    finally:
        # Close connection
        if conn:
            conn.close()
            print(f"\n✓ Database connection closed: {DB_NAME}")

if __name__ == "__main__":
    main()

