

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

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument('--seed', type=int, default=42)
    ap.add_argument('--out', type=str, default='data_raw')
    return ap.parse_args()

def ensure_dir(p): 
    pathlib.Path(p).mkdir(parents=True, exist_ok=True)

def main():
    args = parse_args()
    random.seed(args.seed); np.random.seed(args.seed)
    out = pathlib.Path(args.out); ensure_dir(out)

    fake = Faker('en_AU')

    # Get field names from schema
    field_names_customers = [field.name for field in customers_schema]

    # Prepare output file for Customers
    customers_path = out / 'customers.csv'
    with customers_path.open('w', encoding='utf-8') as f:
        # Write header
        f.write(','.join(field_names_customers) + '\n')

        # Write rows
        for i in range(1, 1001):  # TODO: raise to 80_000
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
    #--------------------------------------------------------CUSTOMERS END--------------------------------------------------------
    
    #--------------------------------------------------------PRODUCTS START--------------------------------------------------------
    # # Prepare output file for products, stores, suppliers, orders (header/lines), sensors
    # Prepare output file for Products
    field_names_products = [field.name for field in products_schema]    
    products_path = out / 'products.csv'
    with products_path.open('w', encoding='utf-8') as f:
        # Write header
        f.write(','.join(field_names_products) + '\n')

        # Write rows
        for i in range(1, 1001):      # TODO: raise to 25000
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
    #--------------------------------------------------------CUSTOMERS END--------------------------------------------------------


    #--------------------------------------------------------STORES START--------------------------------------------------------
    # # Prepare output file for stores
    '''
    field_names_stores = [field.name for field in stores_schema]
    stores_path = out / 'stores1.csv'
    with stores_path.open('w', encoding='utf-8') as f:  
        # Write header
        f.write(','.join(field_names_stores) + '\n')

        # Write rows
        for i in range(1, 101): # TODO: raise to 5000

            if random.random() < 0.05 and store_codes:
                store_code = random.choice(store_codes)
            else:
                store_code = 'STORE-' + rstr.rstr('A-Z0-9', 5)
                store_codes.append(store_code)
 
            store_code = 'STORE-' + rstr.rstr('A-Z0-9', 5)
            open_dt = date(2000, 1, 1) + timedelta(days=random.randint(0, 9000))
            close_dt = (open_dt + timedelta(days=random.randint(100, 5000))) if random.random() < 0.10 else ""
            lat = (-44 + random.random() * 10) if random.random() >= 0.05 else random.choice([999.0, -999.0]) # 5% invalid lat/lon - controlled edge case
            lon = (112 + random.random() * 40) if random.random() >= 0.05 else random.choice([999.0, -999.0])  # 5% invalid lat/lon - controlled edge case
            row = {
                "store_id": i,
                "store_code": store_code,
                "name": fake.company().replace(',', ' '),
                "channel": random.choice(['Online', 'Retail']),
                "region": random.choice(['North', 'South', 'East', 'West']),
                "state": fake.state_abbr(),
                "latitude": f"{lat:.6f}",
                "longitude": f"{lon:.6f}",
                "open_dt": open_dt.isoformat(),
                "close_dt": close_dt.isoformat() if close_dt else ""
            }
            # Write row in schema order
            f.write(','.join(str(row[field]) for field in field_names_stores) + '\n')
    print(f"✅ Store data written to {stores_path} using schema.")   '''
    #--------------------------------------------------------STORES END--------------------------------------------------------
    store_rows_by_code = {}  # store_code → full row
    used_store_codes = set()  # to prevent accidental regeneration
    field_names_stores = [field.name for field in stores_schema]
    stores_path = out / 'stores.csv'

    with stores_path.open('w', encoding='utf-8') as f:  
        f.write(','.join(field_names_stores) + '\n')

        for i in range(1, 101):  # TODO: raise to 5000
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
    #---------------------------------------------------------STORES END--------------------------------------------------------

    #--------------------------------------------------------SUPPLIERS START--------------------------------------------------------
    # # Prepare output file for suppliers
    field_names_suppliers = [field.name for field in suppliers_schema]
    suppliers_path = out / 'suppliers.csv'
    with suppliers_path.open('w', encoding='utf-8') as f:  
        # Write header
        f.write(','.join(field_names_suppliers) + '\n')

        # Write rows
        for i in range(1, 201): # TODO: raise to 8000
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

    


    #--------------------------------------------------------SUPPLIERS END--------------------------------------------------------

    #--------------------------------------------------------ORDERS_HEADER START--------------------------------------------------------
    # Prepare output file for orders header with controlled duplicate order_id entries

    order_rows_by_id = {}  # order_id → full row
    field_names_orders_header = [field.name for field in orders_header_schema]
    orders_header_path = out / 'orders_header.csv'

    with orders_header_path.open('w', encoding='utf-8') as f:
        # Write header
        f.write(','.join(field_names_orders_header) + '\n')

        for i in range(1, 1000):  # TODO: raise to 1000000
            # 1% chance to duplicate an existing order_id
            if random.random() < 0.01 and order_rows_by_id:
                order_id = random.choice(list(order_rows_by_id.keys()))
                row = order_rows_by_id[order_id].copy()
            else:
                order_id = i
                order_dt_local = date(2024, 1, 1) + timedelta(days=random.randint(0, 90))
                order_ts = datetime.combine(order_dt_local, datetime.min.time()) + timedelta(seconds=random.randint(0, 86399))

                # Introduce ~1% foreign key violations
                if random.random() < 0.01:
                    customer_id = random.randint(80001, 90000)  # Invalid
                    store_id = random.randint(5001, 6000)       # Invalid
                else:
                    customer_id = random.randint(1, 1000)
                    store_id = random.randint(1, 100)

                payment_method = random.choice(['Credit Card', 'PayPal', 'Afterpay'])
                shipping_fee = round(random.uniform(0.0, 20.0), 2) # if random.random() > 0.1 else random.choice([-5.0, "N/A"])
                coupon_code = 'COUPON' + rstr.rstr('A-Z0-9', 4) # if random.random() < 0.3 else ""

                row = {
                    "order_id": order_id,
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

                order_rows_by_id[order_id] = row.copy()

            # Write row in schema order
            f.write(','.join(str(row[field]) for field in field_names_orders_header) + '\n')

    print("✅ Orders Header data written with controlled duplicate order_id entries.")
    # Partition by dates is Not implemented yet.
    #--------------------------------------------------------ORDERS_HEADER END--------------------------------------------------------

         





        

            


    



if __name__ == '__main__':
    main()
