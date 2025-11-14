"""
Generate synthetic e-commerce data and save to CSV files.
Creates 5 tables: customers, products, orders, order_items, and reviews.
"""

import pandas as pd
import random
from datetime import datetime, timedelta
from faker import Faker

# Initialize Faker for realistic data generation
fake = Faker()
Faker.seed(42)  # For reproducibility
random.seed(42)

# Configuration
NUM_CUSTOMERS = 100
NUM_PRODUCTS = 50
NUM_ORDERS = 200
MIN_ORDER_ITEMS = 1
MAX_ORDER_ITEMS = 5
REVIEW_PROBABILITY = 0.3  # 30% of orders get reviews

# Product categories
CATEGORIES = [
    'Electronics', 'Clothing', 'Home & Kitchen', 'Books', 'Sports & Outdoors',
    'Beauty & Personal Care', 'Toys & Games', 'Automotive', 'Health & Wellness',
    'Food & Beverages'
]

# Order statuses
ORDER_STATUSES = ['Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled']

# Generate Customers
print("Generating customers...")
customers = []
for i in range(1, NUM_CUSTOMERS + 1):
    join_date = fake.date_between(start_date='-2y', end_date='today')
    customers.append({
        'customer_id': i,
        'name': fake.name(),
        'email': fake.email(),
        'location': f"{fake.city()}, {fake.state_abbr()}",
        'join_date': join_date
    })

customers_df = pd.DataFrame(customers)
customers_df.to_csv('customers.csv', index=False)
print(f"✓ Generated {len(customers)} customers -> customers.csv")

# Generate Products
print("Generating products...")
products = []
for i in range(1, NUM_PRODUCTS + 1):
    category = random.choice(CATEGORIES)
    # Generate product name based on category
    if category == 'Electronics':
        product_name = fake.catch_phrase() + " " + random.choice(['Phone', 'Tablet', 'Laptop', 'Headphones', 'Speaker', 'Camera'])
    elif category == 'Clothing':
        product_name = fake.color_name() + " " + random.choice(['T-Shirt', 'Jeans', 'Jacket', 'Dress', 'Shoes', 'Hat'])
    elif category == 'Home & Kitchen':
        product_name = random.choice(['Coffee Maker', 'Blender', 'Lamp', 'Chair', 'Table', 'Vase']) + " " + fake.word()
    elif category == 'Books':
        product_name = fake.catch_phrase() + " - " + fake.word().title()
    else:
        product_name = fake.catch_phrase()
    
    products.append({
        'product_id': i,
        'product_name': product_name,
        'category': category,
        'price': round(random.uniform(9.99, 999.99), 2),
        'stock_quantity': random.randint(0, 500)
    })

products_df = pd.DataFrame(products)
products_df.to_csv('products.csv', index=False)
print(f"✓ Generated {len(products)} products -> products.csv")

# Generate Orders
print("Generating orders...")
orders = []
order_items = []
order_item_id = 1

# Create a date range for orders (last 18 months)
start_date = datetime.now() - timedelta(days=540)
end_date = datetime.now()

for i in range(1, NUM_ORDERS + 1):
    customer_id = random.randint(1, NUM_CUSTOMERS)
    order_date = fake.date_between(start_date=start_date, end_date=end_date)
    status = random.choice(ORDER_STATUSES)
    
    # Generate order items for this order
    num_items = random.randint(MIN_ORDER_ITEMS, MAX_ORDER_ITEMS)
    order_total = 0
    
    selected_products = random.sample(range(1, NUM_PRODUCTS + 1), min(num_items, NUM_PRODUCTS))
    
    for product_id in selected_products:
        quantity = random.randint(1, 5)
        product = products_df[products_df['product_id'] == product_id].iloc[0]
        price = product['price']
        item_total = price * quantity
        order_total += item_total
        
        order_items.append({
            'order_item_id': order_item_id,
            'order_id': i,
            'product_id': product_id,
            'quantity': quantity,
            'price': round(price, 2)
        })
        order_item_id += 1
    
    orders.append({
        'order_id': i,
        'customer_id': customer_id,
        'order_date': order_date,
        'total_amount': round(order_total, 2),
        'status': status
    })

orders_df = pd.DataFrame(orders)
orders_df.to_csv('orders.csv', index=False)
print(f"✓ Generated {len(orders)} orders -> orders.csv")

order_items_df = pd.DataFrame(order_items)
order_items_df.to_csv('order_items.csv', index=False)
print(f"✓ Generated {len(order_items)} order items -> order_items.csv")

# Generate Reviews
print("Generating reviews...")
reviews = []
review_id = 1

# Generate reviews for some products by customers who ordered them
for order_id in range(1, NUM_ORDERS + 1):
    if random.random() < REVIEW_PROBABILITY:
        # Get order details
        order = orders_df[orders_df['order_id'] == order_id].iloc[0]
        customer_id = order['customer_id']
        order_date = pd.to_datetime(order['order_date'])
        
        # Get products from this order
        items = order_items_df[order_items_df['order_id'] == order_id]
        
        # Review 1-2 products from this order
        num_reviews = random.randint(1, min(2, len(items)))
        reviewed_products = random.sample(items['product_id'].tolist(), num_reviews)
        
        for product_id in reviewed_products:
            # Review date is after order date (within 30 days)
            review_date = fake.date_between(
                start_date=order_date,
                end_date=order_date + timedelta(days=30)
            )
            
            # Rating distribution: mostly positive (4-5 stars), some negative
            rating_weights = [0.05, 0.1, 0.15, 0.3, 0.4]  # 1-5 stars
            rating = random.choices([1, 2, 3, 4, 5], weights=rating_weights)[0]
            
            # Generate comment based on rating
            if rating >= 4:
                comment = fake.sentence(nb_words=random.randint(5, 15))
            elif rating == 3:
                comment = fake.sentence(nb_words=random.randint(3, 10))
            else:
                comment = fake.sentence(nb_words=random.randint(5, 12))
            
            reviews.append({
                'review_id': review_id,
                'product_id': product_id,
                'customer_id': customer_id,
                'rating': rating,
                'comment': comment,
                'review_date': review_date
            })
            review_id += 1

# Also add some standalone reviews (not tied to orders)
for _ in range(20):
    product_id = random.randint(1, NUM_PRODUCTS)
    customer_id = random.randint(1, NUM_CUSTOMERS)
    
    # Check if this customer-product combination already has a review
    existing = [r for r in reviews if r['product_id'] == product_id and r['customer_id'] == customer_id]
    if not existing:
        review_date = fake.date_between(start_date=start_date, end_date=end_date)
        rating_weights = [0.05, 0.1, 0.15, 0.3, 0.4]
        rating = random.choices([1, 2, 3, 4, 5], weights=rating_weights)[0]
        
        if rating >= 4:
            comment = fake.sentence(nb_words=random.randint(5, 15))
        elif rating == 3:
            comment = fake.sentence(nb_words=random.randint(3, 10))
        else:
            comment = fake.sentence(nb_words=random.randint(5, 12))
        
        reviews.append({
            'review_id': review_id,
            'product_id': product_id,
            'customer_id': customer_id,
            'rating': rating,
            'comment': comment,
            'review_date': review_date
        })
        review_id += 1

reviews_df = pd.DataFrame(reviews)
reviews_df.to_csv('reviews.csv', index=False)
print(f"✓ Generated {len(reviews)} reviews -> reviews.csv")

# Summary
print("\n" + "="*50)
print("DATA GENERATION SUMMARY")
print("="*50)
print(f"Customers: {len(customers_df)}")
print(f"Products: {len(products_df)}")
print(f"Orders: {len(orders_df)}")
print(f"Order Items: {len(order_items_df)}")
print(f"Reviews: {len(reviews_df)}")
print("="*50)
print("\nAll CSV files have been generated successfully!")

