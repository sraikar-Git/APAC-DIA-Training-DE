

# Generate synthetic raw data locally with controlled edge cases.
# Usage: python scripts/generate_data.py --seed 42 --out data_raw
import argparse, os, pathlib, random
from datetime import datetime, timedelta, date
import numpy as np
from faker import Faker
from mimesis import Person, Address
import rstr
import pyarrow as pa
import pyarrow.parquet as pq
import xlsxwriter
from schemas import customers_schema , products_schema, stores_schema, suppliers_schema, orders_header_schema
import pandas as pd

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument('--seed', type=int, default=42)
    ap.add_argument('--out', type=str, default='data_raw')
    ap.add_argument('--scale', type=float, default=0.01)
    return ap.parse_args()

def ensure_dir(p): pathlib.Path(p).mkdir(parents=True, exist_ok=True)

def generate_customers(out: pathlib.Path, count: int=1000):
    fake = Faker('en_AU')
    # Get field names from schema
    field_names_customers = [field.name for field in customers_schema]

    # Prepare output file for Customers
    customers_path = out / 'customers.csv'
    with customers_path.open('w', encoding='utf-8') as f:
        # Write header
        f.write(','.join(field_names_customers) + '\n')

        # Write rows
        for i in range(1, count):  # TODO: raise to 80_000
            nk = 'CUST-' + rstr.rstr('A-Z0-9', 8)
            email = fake.email() if random.random() > 0.1 else 'bad_email'  # 10% bad emails - created controlled edge case
            lat = -44 + random.random() * 10
            lon = 112 + random.random() * 40
            birth = date(1960, 1, 1) + timedelta(days=random.randint(0, 20000))
            join_ts = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 400), seconds=random.randint(0, 86399))

            row = {
                "customer_id": i,
                "natural_key": nk,
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": email,
                "phone": fake.phone_number().replace(',', ' '),
                "address_line1": fake.street_address().replace(',', ' '),
                "address_line2": "",
                "city": fake.city().replace(',', ' '),
                "state_region": fake.state_abbr(),
                "postcode": fake.postcode(),
                "country_code": "AU",
                "latitude": f"{lat:.6f}",
                "longitude": f"{lon:.6f}",
                "birth_date": birth.isoformat(),
                "join_ts": join_ts.isoformat(),
                "is_vip": str(random.random() < 0.15),
                "gdpr_consent": str(random.random() > 0.05)
            }

            # Write row in schema order
            f.write(','.join(str(row[field]) for field in field_names_customers) + '\n')
    print(f"✅ Customer data written to {customers_path} using schema.")

def generate_products(out: pathlib.Path, count: int=1000):
    field_names_products = [field.name for field in products_schema]   
    fake = Faker('en_AU') 
    products_path = out / 'products.csv'
    with products_path.open('w', encoding='utf-8') as f:
        # Write header
        f.write(','.join(field_names_products) + '\n')

        # Write rows
        for i in range(1, count):      # TODO: raise to 25000
            sku = 'SKU-' + rstr.rstr('A-Z0-9', 6)
            current_price = round(random.uniform(10.0, 1000.0), 2) if random.random() > 0.1 else random.choice([0.0, "N/A"])  # 10% zero/invalid prices - controlled edge case
            is_discontinued = random.random() < 0.10
            introduced_dt = date(2010, 1, 1) + timedelta(days=random.randint(0, 5000))
            discontinued_dt = (introduced_dt + timedelta(days=random.randint(100, 2000))) if is_discontinued else ""
            row = {
                "product_id": i,
                "sku": sku,
                "name": fake.word().title(),
                "category": random.choice(['Electronics', 'Clothing', 'Home', 'Sports']),
                "subcategory": random.choice(['Sub1', 'Sub2', 'Sub3']),
                "current_price": current_price,
                "currency": 'AUD',
                "is_discontinued": str(is_discontinued),
                "introduced_dt": introduced_dt.isoformat(),
                "discontinued_dt": discontinued_dt.isoformat() if discontinued_dt else ""
            }
            # Write row in schema order
            f.write(','.join(str(row[field]) for field in field_names_products) + '\n')
    print(f"✅ Product data written to {products_path} using schema.")

def generate_stores(out: pathlib.Path, count: int = 100):
    store_rows_by_code = {}  # store_code → full row
    used_store_codes = set()  # to prevent accidental regeneration
    field_names_stores = [field.name for field in stores_schema]
    fake = Faker('en_AU')
    stores_path = out / 'stores.csv'

    with stores_path.open('w', encoding='utf-8') as f:  
        f.write(','.join(field_names_stores) + '\n')

        for i in range(1, count):  # TODO: raise to 5000
            # 5% chance to duplicate an existing store
            if random.random() < 0.05 and store_rows_by_code:
                store_code = random.choice(list(store_rows_by_code.keys()))
                row = store_rows_by_code[store_code].copy()
                row["store_id"] = i
            else:
                # Generate a truly unique store_code
                while True:
                    store_code = 'STORE-' + rstr.rstr('A-Z0-9', 5)
                    if store_code not in used_store_codes:
                        used_store_codes.add(store_code)
                        break

                open_dt = date(2000, 1, 1) + timedelta(days=random.randint(0, 9000))
                close_dt = (open_dt + timedelta(days=random.randint(100, 5000))) if random.random() < 0.10 else ""

                lat = (-44 + random.random() * 10) if random.random() >= 0.05 else random.choice([999.0, -999.0])
                lon = (112 + random.random() * 40) if random.random() >= 0.05 else random.choice([999.0, -999.0])

                row = {
                    "store_id": i,
                    "store_code": store_code,
                    "name": fake.company().replace(',', ' '),
                    "channel": random.choice(['Online', 'Retail']),
                    "region": random.choice(['North', 'South', 'East', 'West']),
                    "state": fake.state_abbr(),
                    "latitude": f"{lat:.6f}",
                    "longitude": f"{lon:.6f}",
                    "open_dt": open_dt.strftime('%d/%m/%Y'),
                    "close_dt": close_dt.strftime('%d/%m/%Y') if close_dt else ""
                }

                store_rows_by_code[store_code] = row.copy()

            f.write(','.join(str(row[field]) for field in field_names_stores) + '\n')

    print(f"✅ Store data written to {stores_path} using schema.")

def generate_suppliers(out: pathlib.Path, count: int = 200):
    # Prepare output file for suppliers
    field_names_suppliers = [field.name for field in suppliers_schema]
    fake = Faker('en_AU')
    suppliers_path = out / 'suppliers.csv'
    with suppliers_path.open('w', encoding='utf-8') as f:  
        # Write header
        f.write(','.join(field_names_suppliers) + '\n')
  
        # Write rows
        for i in range(1, count): # TODO: raise to 8000
            supplier_code = 'SUP-' + rstr.rstr('A-Z0-9', 5)
            lead_time_days = random.randint(1, 30) if random.random() > 0.1 else random.choice([0, -5])
            row = {
                "supplier_id": i,
                "supplier_code": supplier_code,
                "name": fake.company().replace(',', ' '),
                "country_code": random.choice(['AU', 'CN', 'US', 'UK']),
                "lead_time_days": lead_time_days,
                "preferred": str(random.random() < 0.20)
            }
            # Write row in schema order
            f.write(','.join(str(row[field]) for field in field_names_suppliers) + '\n')
    print(f"✅ Supplier data written to {suppliers_path} using schema.")

def generate_orders_header(out: pathlib.Path, count: int):
    ## Step 1: Read max IDs from reference files
    customers_df = pd.read_csv(out / 'customers.csv')
    stores_df = pd.read_csv(out / 'stores.csv')

    max_customer_id = customers_df['customer_id'].max()
    max_store_id = stores_df['store_id'].max()

    print(f"Max customer_id: {max_customer_id}, Max store_id: {max_store_id}")

    # Step 2: Setup
    field_names_orders_header = [field.name for field in orders_header_schema]
    base_dir = out / 'orders_partitioned'
    base_dir.mkdir(parents=True, exist_ok=True)

    total_rows = 1000000
    target_violations = int(total_rows * 0.01)      # 1% = 15
    target_duplicates = int(total_rows * 0.0005)    # 0.05% = 1
    print(f"Target FK violations: {target_violations}, Target duplicates: {target_duplicates}")

    violations = 0
    duplicates = 0
    order_rows_by_id = {}
    rows_written = 0
    next_order_id = 1

    # Step 3: Generate valid rows first
    generated_rows = []

    while rows_written < total_rows - target_duplicates:
        order_dt_local = date(2024, 1, 1) + timedelta(days=random.randint(0, 90))
        order_ts = datetime.combine(order_dt_local, datetime.min.time()) + timedelta(seconds=random.randint(0, 86399))

        # Inject FK violations if quota not met
        if violations < target_violations:
            customer_id = random.randint(max_customer_id + 1, max_customer_id + 100)
            store_id = random.randint(max_store_id + 1, max_store_id + 100)
            violations += 1
        else:
            customer_id = random.randint(1, max_customer_id)
            store_id = random.randint(1, max_store_id)

        payment_method = random.choice(['Credit Card', 'PayPal', 'Afterpay'])
        shipping_fee = round(random.uniform(0.0, 20.0), 2)
        coupon_code = 'COUPON' + rstr.rstr('A-Z0-9', 4)

        row = {
            "order_id": next_order_id,
            "order_ts": order_ts.isoformat(),
            "order_dt_local": order_dt_local.isoformat(),
            "customer_id": customer_id,
            "store_id": store_id,
            "channel": random.choice(['Online', 'Retail']),
            "payment_method": payment_method,
            "coupon_code": coupon_code,
            "shipping_fee": shipping_fee,
            "currency": "AUD"
        }

        order_rows_by_id[next_order_id] = row.copy()
        generated_rows.append(row)
        next_order_id += 1
        rows_written += 1

    # Step 4: Inject exact number of duplicates
    duplicate_ids = random.sample(list(order_rows_by_id.keys()), target_duplicates)
    for dup_id in duplicate_ids:
        dup_row = order_rows_by_id[dup_id].copy()
        dup_row["order_id"] = next_order_id
        generated_rows.append(dup_row)
        next_order_id += 1
        duplicates += 1

    ## Step 5: Write rows partitioned by date using parallelism
    # Group rows by date
    rows_by_date = defaultdict(list)
    for row in generated_rows:
        date_str = row["order_dt_local"]
        rows_by_date[date_str].append(row)
    
    def write_partition(date_str, rows, base_dir, field_names):
        partition_dir = base_dir / date_str
        partition_dir.mkdir(parents=True, exist_ok=True)
        partition_file = partition_dir / 'orders_header.csv'

        with partition_file.open('w', encoding='utf-8') as f:
            f.write(','.join(field_names) + '\n')
            for row in rows:
                f.write(','.join(str(row[field]) for field in field_names) + '\n')

    with ThreadPoolExecutor() as executor:
        for date_str, rows in rows_by_date.items():
            executor.submit(write_partition, date_str, rows, base_dir, field_names_orders_header)
    
        print(f"✅ Orders_header data written to {base_dir} using schema.")


def main():
    args = parse_args()
    random.seed(args.seed)
    np.random.seed(args.seed)
    out = pathlib.Path(args.out)
    ensure_dir(out)

    # Create samples folder inside output
    samples_dir = out / 'samples'
    ensure_dir(samples_dir)

    # total row counts
    customer_count = int(80000+1)
    product_count = int(25000+1)    
    store_count = int(5000+1) 
    supplier_count = int(8000+1)
    orders_header_count = int(1000000+1)    


    # scale row counts
    customer_sample = int(customer_count * args.scale)
    product_sample = int(product_count * args.scale)
    store_sample = int(store_count * args.scale)
    supplier_sample = int(supplier_count * args.scale)
    orders_header_sample = int(orders_header_count * args.scale)


    print(customer_sample,"customer_sample",
          store_sample,"store_sample",
          product_sample,"product_sample",
          supplier_sample,"supplier_sample",
          orders_header_count,"orders_header_sample"
        )

    # Generate scaled data into samples/
    generate_customers(samples_dir, customer_sample)
    generate_products(samples_dir, product_sample)
    generate_stores(samples_dir, store_sample)
    generate_suppliers(samples_dir, supplier_sample)
    generate_orders_header(samples_dir, orders_header_sample)

    

    ## Generate full data into out/
    #generate_customers(out,customer_count)
    #generate_products(out,product_count)
    #generate_stores(out,store_count)
    #generate_suppliers(out,supplier_count)
    generate_orders_header(out,orders_header_count)


   
    print(f"✅ Sample raw written to {out}. Expand to required volumes per /docs.")

if __name__ == '__main__':
    main()
