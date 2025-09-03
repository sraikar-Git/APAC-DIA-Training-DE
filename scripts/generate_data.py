# Generate synthetic raw data locally with controlled edge cases.
# Usage: python scripts/generate_data.py --seed 42 --out data_raw
import json
import argparse, os, pathlib, random
from datetime import datetime, timedelta, date
import uuid
from deltalake import write_deltalake
import numpy as np
from faker import Faker
from mimesis import Person, Address
import rstr
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import xlsxwriter
from schemas import customers_schema , products_schema, stores_schema, suppliers_schema, orders_header_schema, orders_lines_schema, sensors_schema, events_schema, shipments_schema , returns_day1_schema
import pandas as pd
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
from deltalake import DeltaTable
import shutil
import psutil
import os

def print_memory_usage(stage):
    process = psutil.Process(os.getpid())
    mem_mb = process.memory_info().rss / (1024 * 1024)
    print(f"🧠 Memory usage {stage}: {mem_mb:.2f} MB")

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument('--seed', type=int, default=42)
    ap.add_argument('--out', type=str, default='data_raw')
    ap.add_argument('--scale', type=float, default=0.01)
    return ap.parse_args()

def ensure_dir(p): pathlib.Path(p).mkdir(parents=True, exist_ok=True)


def generate_customers(out: pathlib.Path, count: int):
    fake = Faker('en_AU')
    customers_path = out / 'customers.csv'

    # --- Generate core fields ---
    customer_ids = np.arange(1, count + 1)
    natural_keys = ['CUST-' + rstr.rstr('A-Z0-9', 8) for _ in range(count)]

    # Inject ~0.2% duplicates
    dup_count = int(count * 0.002)
    for i in range(dup_count):
        natural_keys[i] = natural_keys[-(i + 1)]

    # Generate emails with 0.5–1% malformed
    bad_email_count = random.randint(int(count * 0.005), int(count * 0.01))
    emails = [fake.email() for _ in range(count)]
    for i in random.sample(range(count), bad_email_count):
        emails[i] = "invalid_email"

    # Generate location and metadata
    latitudes = np.round(-44 + np.random.rand(count) * 10, 6)
    longitudes = np.round(112 + np.random.rand(count) * 40, 6)
    birth_dates = [date(1960, 1, 1) + timedelta(days=random.randint(0, 20000)) for _ in range(count)]
    join_timestamps = [datetime(2024, 1, 1) + timedelta(days=random.randint(0, 400), seconds=random.randint(0, 86399)) for _ in range(count)]
    is_vip_flags = [random.random() < 0.15 for _ in range(count)]
    gdpr_consents = [random.random() > 0.05 for _ in range(count)]

    # Generate personal and address fields
    first_names = [fake.first_name() for _ in range(count)]
    last_names = [fake.last_name() for _ in range(count)]
    phones = [fake.phone_number().replace(',', ' ') for _ in range(count)]
    address_line1 = [fake.street_address().replace(',', ' ') for _ in range(count)]
    address_line2 = ["" for _ in range(count)]
    cities = [fake.city().replace(',', ' ') for _ in range(count)]
    states = [fake.state_abbr() for _ in range(count)]
    postcodes = [fake.postcode() for _ in range(count)]
    countries = ["AU"] * count

    # Inject nulls into phone and address_line1
    null_count = int(count * 0.01)
    for i in random.sample(range(count), null_count):
        phones[i] = ""
    for i in random.sample(range(count), null_count):
        address_line1[i] = ""

    # --- Build DataFrame ---
    df = pd.DataFrame({
        "customer_id": customer_ids,
        "natural_key": natural_keys,
        "first_name": first_names,
        "last_name": last_names,
        "email": emails,
        "phone": phones,
        "address_line1": address_line1,
        "address_line2": address_line2,
        "city": cities,
        "state_region": states,
        "postcode": postcodes,
        "country_code": countries,
        "latitude": latitudes,
        "longitude": longitudes,
        "birth_date": [d.isoformat() for d in birth_dates],
        "join_ts": [ts.isoformat() for ts in join_timestamps],
        "is_vip": is_vip_flags,
        "gdpr_consent": gdpr_consents
    })

    # --- Write to CSV ---
    df.to_csv(customers_path, index=False)
    print(f"✅ Customer data with anomalies written to {customers_path} ({count} rows)")


def generate_products(out: pathlib.Path, count: int):
    field_names_products = [field.name for field in products_schema]
    fake = Faker('en_AU')
    products_path = out / 'products.csv'

    # Determine how many prices will be invalid
    invalid_price_count = random.randint(int(count * 0.001), int(count * 0.005))
    invalid_price_indices = set(random.sample(range(1, count), invalid_price_count))

    # Determine how many discontinued products will have null discontinued_dt
    discontinued_null_count = random.randint(int(count * 0.005), int(count * 0.01))
    discontinued_null_indices = set(random.sample(range(1, count), discontinued_null_count))

    with products_path.open('w', encoding='utf-8') as f:
        # Write header
        f.write(','.join(field_names_products) + '\n')

        # Write rows
        for i in range(1, count):
            sku = 'SKU-' + rstr.rstr('A-Z0-9', 6)

            # Inject invalid or missing prices
            if i in invalid_price_indices:
                current_price = random.choice([None, "N/A", 0.0])
            else:
                current_price = round(random.uniform(10.0, 1000.0), 2)

            is_discontinued = random.random() < 0.10
            introduced_dt = date(2010, 1, 1) + timedelta(days=random.randint(0, 5000))

            # Inject null discontinued_dt for some discontinued products
            if is_discontinued and i in discontinued_null_indices:
                discontinued_dt = ""
            elif is_discontinued:
                discontinued_dt = introduced_dt + timedelta(days=random.randint(100, 2000))
            else:
                discontinued_dt = ""

            row = {
                "product_id": i,
                "sku": sku,
                "name": fake.word().title(),
                "category": random.choice(['Electronics', 'Clothing', 'Home', 'Sports']),
                "subcategory": random.choice(['Sub1', 'Sub2', 'Sub3']),
                "current_price": current_price if current_price is not None else "",
                "currency": 'AUD',
                "is_discontinued": str(is_discontinued),
                "introduced_dt": introduced_dt.isoformat(),
                "discontinued_dt": discontinued_dt.isoformat() if discontinued_dt else ""
            }

            # Write row in schema order
            f.write(','.join(str(row[field]) for field in field_names_products) + '\n')

    print(f"✅ Product data written to {products_path} with {invalid_price_count} invalid prices and {discontinued_null_count} null discontinued_dt entries.")


def generate_stores(out: pathlib.Path, count: int):
    # Step 1: Extract field names from schema
    field_names_stores = [field.name for field in stores_schema]

    # Step 2: Setup output path
    stores_csv = out / "stores.csv"
    out.mkdir(parents=True, exist_ok=True)

    # Step 3: Generate base fields
    store_ids = np.arange(1, count + 1)
    store_codes = [f"STR{random.randint(1000, 9999)}" for _ in range(count)]
    names = [f"Store {i}" for i in store_ids]
    channels = np.random.choice(["web", "pos"], size=count)
    regions = np.random.choice(["North", "South", "East", "West"], size=count)
    states = np.random.choice(["NSW", "VIC", "QLD", "WA", "SA", "TAS"], size=count)

    # Step 4: Generate location with anomalies
    latitudes = np.random.uniform(-38.0, -10.0, size=count)
    longitudes = np.random.uniform(110.0, 155.0, size=count)

    for i in random.sample(range(count), k=10):
        latitudes[i] = random.choice([999.0, -999.0])
        longitudes[i] = random.choice([999.0, -999.0])

    # Step 5: Generate lifecycle dates
    open_dates = [datetime(2000, 1, 1) + timedelta(days=random.randint(0, 8000)) for _ in range(count)]
    close_dates = [dt + timedelta(days=random.randint(1000, 3000)) if random.random() < 0.2 else None for dt in open_dates]

    # Step 6: Inject duplicate store_codes
    for i in random.sample(range(count), k=15):
        store_codes[i] = store_codes[random.randint(0, count - 1)]

    # Step 7: Assemble DataFrame
    df = pd.DataFrame({
        "store_id": store_ids,
        "store_code": store_codes,
        "name": names,
        "channel": channels,
        "region": regions,
        "state": states,
        "latitude": latitudes,
        "longitude": longitudes,
        "open_dt": [dt.strftime("%Y-%m-%d") for dt in open_dates],
        "close_dt": [dt.strftime("%Y-%m-%d") if dt else None for dt in close_dates]
    })

    # Step 8: Align with schema field names
    df = df[field_names_stores]

    # Step 9: Write to CSV
    df.to_csv(stores_csv, index=False)
    print(f"✅ Generated {count} stores → {stores_csv}")


def generate_suppliers(out: pathlib.Path, count: int):
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

    # print(f"Max customer_id: {max_customer_id}, Max store_id: {max_store_id}")

    # Step 2: Setup
    field_names_orders_header = [field.name for field in orders_header_schema]
    base_dir = out / 'orders_header'
    base_dir.mkdir(parents=True, exist_ok=True)

    total_rows = 1000000
    target_violations = int(total_rows * 0.01)      # 1%
    target_duplicates = int(total_rows * 0.0005)    # 0.05% 
    # print(f"Target FK violations: {target_violations}, Target duplicates: {target_duplicates}")

    violations = 0;   duplicates = 0;    order_rows_by_id = {};    rows_written = 0;    next_order_id = 1

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


def generate_orders_lines(out: pathlib.Path, count: int):

    #print(datetime.now(),"Start of orders_lines generation")

    # Step 1: Load reference orders_header
    orders_base_dir = out / 'orders_header'
    order_files = list(orders_base_dir.glob("*/orders_header.csv"))
    assert order_files, "No orders_header files found for partitioning."

     ## Step 2: Read max PRODUCT IDs from reference files
    products_df = pd.read_csv(out / 'products.csv')
    max_product_id = products_df['product_id'].max()
    valid_product_ids = products_df['product_id'].astype(int).tolist()
    #print(f"Max product_id: {max_product_id}")

    # Load all orders 
    orders = []
    for file in order_files:
        df = pd.read_csv(file)
        orders.extend(df.to_dict(orient='records'))

    #print(f"Loaded {len(orders)} orders for line generation.")

    # Step 2: Setup
    base_dir = out / 'order_lines'
    base_dir.mkdir(parents=True, exist_ok=True)

    target_invalid_products = int(count * 0.01)  # 1%
    violations = 0
    lines_written = 0
    next_line_number = 1

    # Step 3: Generate line items
    generated_lines = []
    while lines_written < count:
        order = random.choice(orders)
        order_id = order["order_id"]
        order_dt_local = order["order_dt_local"]

        # Inject FK violations/product_ids 
        if violations < target_invalid_products:
            product_id = random.randint(max_product_id + 1, max_product_id + 10000) # invalid product_id
            violations += 1
        else:
            product_id = random.choice(valid_product_ids) # valid product_id range from products.csv
            
        
        qty = random.randint(-2, 10) if random.random() < 0.001 else random.randint(1, 10)
        unit_price = 0.0 if random.random() < 0.001 else round(random.uniform(5.0, 500.0), 4)
        line_discount_pct = round(random.uniform(0.0, 0.25), 4)
        tax_pct = round(random.uniform(0.05, 0.15), 4)

        row = {
            "order_id": order_id,
            "line_number": next_line_number,
            "product_id": product_id,
            "qty": qty,
            "unit_price": unit_price,
            "line_discount_pct": line_discount_pct,
            "tax_pct": tax_pct
        }

        generated_lines.append((order_dt_local, row))
        next_line_number += 1
        lines_written += 1

    # print(f"✅ Generated {lines_written} order lines with {violations} invalid product_ids.")

    # Step 4: Partitioned write
    rows_by_date = defaultdict(list)
    for date_str, row in generated_lines:
        rows_by_date[date_str].append(row)

    def write_partition(date_str, rows, base_dir):
        partition_dir = base_dir / date_str
        partition_dir.mkdir(parents=True, exist_ok=True)
        partition_file = partition_dir / 'order_lines.csv'

        with partition_file.open('w', encoding='utf-8') as f:
            f.write("order_id,line_number,product_id,qty,unit_price,line_discount_pct,tax_pct\n")
            for row in rows:
                f.write(','.join(str(row[k]) for k in row.keys()) + '\n')

    with ThreadPoolExecutor() as executor:
        for date_str, rows in rows_by_date.items():
            executor.submit(write_partition, date_str, rows, base_dir)

    print(f"✅ Order lines written to {base_dir} with partitioning by order_dt_local.")
    #print(datetime.now(),"end of orders_lines generation")


def generate_sensors(out: pathlib.Path, count: int):

    # print(datetime.now(),"Start of sensors data generation")
    # Step 1: Load reference store IDs and shelf IDs
    stores_df = pd.read_csv(out / 'stores.csv')
    store_ids = np.array(stores_df['store_id'].tolist())
    shelf_ids = np.array([f"SHELF-{rstr.rstr('A-Z0-9', 4)}" for _ in range(100)])

    #print(f"Loaded {len(store_ids)} store_ids and {len(shelf_ids)} shelf_ids")

    # Step 2: Setup
    base_dir = out / 'sensors'
    base_dir.mkdir(parents=True, exist_ok=True)

    # Step 3: Generate fields using NumPy
    rng = np.random.default_rng(seed=42)

    # Random store and shelf IDs
    store_id_arr = rng.choice(store_ids, size=count)
    shelf_id_arr = rng.choice(shelf_ids, size=count)

    # Timestamps
    start_ts = datetime(2025, 1, 1).timestamp()
    end_ts = datetime(2025, 4, 1).timestamp()
    ts_arr = rng.integers(start_ts, end_ts, size=count)
    sensor_ts_arr = np.array([datetime.fromtimestamp(ts).isoformat() for ts in ts_arr])

    # Inject occasional missing timestamps
    missing_ts_mask = rng.random(count) < 0.0005
    sensor_ts_arr[missing_ts_mask] = ""

    # Temperature and humidity
    temperature_arr = rng.uniform(0.0, 40.0, size=count).round(2)
    humidity_arr = rng.uniform(20.0, 80.0, size=count).round(2)

    # Inject anomalies
    anomaly_count = int(count * rng.uniform(0.001, 0.005))
    anomaly_indices = rng.choice(count, size=anomaly_count, replace=False)
    temperature_arr[anomaly_indices] = rng.uniform(-10.0, 60.0, size=anomaly_count).round(2)
    humidity_arr[anomaly_indices] = rng.uniform(-5.0, 120.0, size=anomaly_count).round(2)

    # Battery voltage
    battery_arr = rng.integers(3000, 4200, size=count)

    # Step 4: Assemble rows
    rows = []
    for i in range(count):
        ts = sensor_ts_arr[i]
        month_str = datetime.fromtimestamp(ts_arr[i]).strftime("%Y-%m") if ts else "unknown"
        partition_key = f"store_id={store_id_arr[i]}/month={month_str}"

        row = {
            "sensor_ts": ts,
            "store_id": store_id_arr[i],
            "shelf_id": shelf_id_arr[i],
            "temperature_c": temperature_arr[i],
            "humidity_pct": humidity_arr[i],
            "battery_mv": battery_arr[i]
        }

        rows.append((partition_key, row))

    # print(f"Generated {count} sensor rows with {anomaly_count} anomalies")

    # Step 5: Group rows by partition
    rows_by_partition = defaultdict(list)
    for partition_key, row in rows:
        rows_by_partition[partition_key].append(row)

    # Step 6: Write partitions in parallel
    def write_partition(partition_key, rows, base_dir):
        partition_dir = base_dir / partition_key
        partition_dir.mkdir(parents=True, exist_ok=True)
        partition_file = partition_dir / 'sensors.csv'

        with partition_file.open('w', encoding='utf-8') as f:
            f.write("sensor_ts,store_id,shelf_id,temperature_c,humidity_pct,battery_mv\n")
            for row in rows:
                f.write(','.join(str(row[k]) for k in row.keys()) + '\n')

    with ThreadPoolExecutor() as executor:
        for partition_key, rows in rows_by_partition.items():
            executor.submit(write_partition, partition_key, rows, base_dir)

    print(f"✅ Sensor data written to {base_dir} partitioned by store_id and month.")


def generate_events(out: pathlib.Path, count: int):

    # Step 1: Load reference data
    customers_df = pd.read_csv(out / 'customers.csv')
    products_df = pd.read_csv(out / 'products.csv')
    user_ids = customers_df['customer_id'].tolist()
    product_ids = products_df['product_id'].tolist()

    # print(f"Loaded {len(user_ids)} user_ids and {len(product_ids)} product_ids")

    # Step 2: Setup
    base_dir = out / 'events'
    base_dir.mkdir(parents=True, exist_ok=True)

    malformed_count = int(count * 0.0005)  # 0.05%
    missing_envelope_chance = 0.0005       # ~0.05%
    rng = np.random.default_rng(seed=42)

    events_by_date = defaultdict(list)

    for i in range(count):
        # Generate timestamp
        event_dt = datetime(2025, 1, 1) + timedelta(days=random.randint(0, 90), seconds=random.randint(0, 86399))
        event_ts = event_dt.isoformat()
        event_date = event_dt.strftime("%Y-%m-%d")

        # Envelope
        envelope = {
            "event_id": str(uuid.uuid4()),
            "event_ts": event_ts,
            "event_type": random.choice(["click", "view", "purchase", "login"]),
            "user_id": random.choice(user_ids),
            "session_id": str(uuid.uuid4())
        }

        # Occasionally drop a required envelope field
        if random.random() < missing_envelope_chance:
            del envelope[random.choice(list(envelope.keys()))]

        # Payload using product reference
        payload = {
            "product_id": random.choice(product_ids),
            "page": random.choice(["home", "search", "product", "checkout"]),
            "referrer": random.choice(["google", "email", "social", "direct"]),
            "device": random.choice(["mobile", "desktop", "tablet"]),
            "metadata": {
                "browser": random.choice(["Chrome", "Safari", "Firefox"]),
                "version": rstr.rstr("0-9.", 5)
            }
        }

        # Combine envelope + payload
        event_obj = {
            "envelope": envelope,
            "payload": payload
        }

        # Inject malformed JSON
        if i < malformed_count:
            json_line = '{"event_id": "MALFORMED", "event_ts": "BROKEN"'  # intentionally broken
        else:
            json_line = json.dumps(event_obj)

        events_by_date[event_date].append(json_line)

    # print(f"Generated {count} events with {malformed_count} malformed lines")

    # Step 3: Write partitions in parallel
    def write_partition(date_str, lines, base_dir):
        partition_dir = base_dir / f"event_date={date_str}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        file_path = partition_dir / 'events.jsonl'

        with file_path.open('w', encoding='utf-8') as f:
            for line in lines:
                f.write(line + '\n')

    with ThreadPoolExecutor() as executor:
        for date_str, lines in events_by_date.items():
            executor.submit(write_partition, date_str, lines, base_dir)

    print(f"✅ Events written to {base_dir} partitioned by event date.")


def generate_exchange_rates(out: pathlib.Path):
    # Step 1: Setup
    start_date = date(2022, 1, 1)
    end_date = date(2024, 12, 31)
    currencies = ['USD', 'EUR', 'JPY', 'GBP', 'NZD', 'CAD', 'CHF', 'CNY', 'SGD', 'INR']
    base_currency = 'AUD'

    # Step 2: Generate date range including weekends
    all_dates = pd.date_range(start=start_date, end=end_date, freq='D')
    # print(f"Generating exchange rates for {len(all_dates)} days across {len(currencies)} currencies")

    # Step 3: Simulate exchange rates
    data = []
    rng = np.random.default_rng(seed=42)

    # Create base rates for each currency
    base_rates = {cur: rng.uniform(0.5, 2.0) for cur in currencies}

    for cur in currencies:
        rate = base_rates[cur]
        for d in all_dates:
            # Simulate small daily fluctuation
            rate += rng.normal(0, 0.005)
            rate = max(rate, 0.01)  # prevent negative or zero rates
            data.append({
                "date": d.date(),
                "currency": cur,
                "rate_to_aud": round(rate, 8)
            })

    # Step 4: Write to Excel
    output_file = out / 'exchange_rates.xlsx'
    df = pd.DataFrame(data)
    df.sort_values(by=["date", "currency"], inplace=True)

    with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='ExchangeRates', index=False)

    print(f"✅ Exchange rates written to {output_file}")


def generate_shipments(out: pathlib.Path, count: int):

    # Step 1: Load all order_ids from partitioned orders_header
    orders_dir = out / 'orders_header'
    order_files = list(orders_dir.glob("*/orders_header.csv"))
    assert order_files, "No orders_header.csv files found in partitioned folders."

    orders_df = pd.concat([pd.read_csv(f) for f in order_files], ignore_index=True)
    order_ids = orders_df['order_id'].tolist()
    # print(f"Loaded {len(order_ids)} order_ids from {len(order_files)} partitions")

    # Step 2: Setup
    carriers = ['AUSPOST', 'Sendle', 'DHL', 'FedEx']
    sla_days = 3
    anomaly_rate = 0.01
    in_transit_rate = 0.02

    rng = np.random.default_rng(seed=42)
    shipment_ids = np.arange(1, count + 1)
    order_id_arr = rng.choice(order_ids, size=count)
    carrier_arr = rng.choice(carriers, size=count)

    base_date = datetime(2025, 1, 1)
    shipped_at_arr = [base_date + timedelta(days=int(x), seconds=int(rng.integers(0, 86400))) for x in rng.integers(0, 180, size=count)]

    delivered_at_arr = []
    for i in range(count):
        shipped = shipped_at_arr[i]
        if rng.random() < in_transit_rate:
            delivered_at_arr.append(None)
        elif rng.random() < anomaly_rate:
            late_offset = int(rng.integers(3, 10).item())
            delivered_at_arr.append(shipped + timedelta(days=sla_days + late_offset))    
        else:
            days_offset = int(rng.integers(1, sla_days).item())
            delivered_at_arr.append(shipped + timedelta(days=days_offset))
                 

    ship_cost_arr = rng.uniform(5.0, 50.0, size=count).round(2)

    # Step 3: Build Arrow Table - extract schema from schema file
    columns = {}
    for field in shipments_schema:
        name = field.name
        dtype = field.type

        if name == "shipment_id":
            columns[name] = pa.array(shipment_ids, type=dtype)
        elif name == "order_id":
            columns[name] = pa.array(order_id_arr, type=dtype)
        elif name == "carrier":
            columns[name] = pa.array(carrier_arr, type=dtype)
        elif name == "shipped_at":
            columns[name] = pa.array(shipped_at_arr, type=dtype)
        elif name == "delivered_at":
            columns[name] = pa.array(delivered_at_arr, type=dtype)
        elif name == "ship_cost":
            columns[name] = pa.array(ship_cost_arr, type=pa.float64()).cast(dtype)

    table = pa.table(columns, schema=shipments_schema)


    # Step 4: Write to Parquet
    out_dir = out 
    out_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out_dir / 'shipments.parquet', compression='snappy')

    print(f"✅ Shipments written to {out_dir / 'shipments.parquet'} with {int(in_transit_rate * count)} in-transit and {int(anomaly_rate * count)} late deliveries.")


def generate_returns(out: pathlib.Path, count: int):
    # Step 1: Load reference orders_header
    orders_base_dir = out / "orders_header"
    order_files = list(orders_base_dir.glob("*/orders_header.csv"))
    assert order_files, "No orders_header files found for partitioning."

    orders_df = pd.concat([pd.read_csv(f) for f in order_files], ignore_index=True)
    valid_order_ids = orders_df["order_id"].dropna().astype(int).tolist()
    # print(f"✅ Loaded {len(valid_order_ids)} order IDs from {len(order_files)} partitions")

    # Step 2: Load reference products
    products_df = pd.read_csv(out / "products.csv")
    valid_product_ids = products_df["product_id"].dropna().astype(int).tolist()
    max_product_id = max(valid_product_ids)
    # print(f"✅ Loaded {len(valid_product_ids)} product IDs (max ID: {max_product_id})")

    # Step 3: Setup paths
    delta_path = out / "returns_delta"
    base_rows = int(count * 0.9)
    evolved_rows = count - base_rows

    # Step 4: Clean up existing Delta table
    if delta_path.exists():
        shutil.rmtree(delta_path)

    # Step 5: Generate base returns (v1)
    def generate_base(n):
        now = datetime.now()
        order_ids = np.random.choice(valid_order_ids, size=n, replace=True)
        product_ids = np.random.choice(valid_product_ids, size=n, replace=True)

        df = pd.DataFrame({
            "return_id": np.arange(1, n + 1),
            "order_id": order_ids,
            "product_id": product_ids,
            "return_ts": [now - timedelta(days=np.random.randint(0, 30)) for _ in range(n)],
            "qty": np.random.randint(1, 5, size=n),
            "reason": np.random.choice(["damaged", "wrong item", "no longer needed"], size=n),
            "return_reason_code": np.nan  # Schema alignment
        })
        return df

    base_df = generate_base(base_rows)
    write_deltalake(str(delta_path), base_df, mode="overwrite")
    print(f"✅ Returns - base version written: {base_rows} rows")

    # Step 6: Generate evolved returns (v2)
    def generate_evolved(start_id, n):
        now = datetime.now()
        order_ids = np.random.choice(valid_order_ids, size=n, replace=True)
        product_ids = np.random.choice(valid_product_ids, size=n, replace=True)

        return pd.DataFrame({
            "return_id": np.arange(start_id, start_id + n),
            "order_id": order_ids,
            "product_id": product_ids,
            "return_ts": [now - timedelta(days=np.random.randint(0, 30)) for _ in range(n)],
            "qty": np.random.randint(1, 5, size=n),
            "reason": np.random.choice(["damaged", "wrong item", "no longer needed"], size=n),
            "return_reason_code": np.random.randint(1, 5, size=n).astype(float)
        })

    evolved_df = generate_evolved(start_id=base_rows + 1, n=evolved_rows)
    combined_df = pd.concat([base_df, evolved_df], ignore_index=True)
    write_deltalake(str(delta_path), combined_df, mode="overwrite")
    print(f"Return- merged version written: {len(combined_df)} total rows")
    
    # Step 7: Demonstrate UPSERT
    def upsert_returns(delta_path: pathlib.Path):
        dt = DeltaTable(str(delta_path))

        # Simulate updates: modify qty for some existing return_ids
        existing_ids = dt.to_pandas()["return_id"].sample(10).tolist()
        # print("Sampled existing_ids for UPSERT:", existing_ids)  # <-- Add this line
        upsert_df = pd.DataFrame({
            "return_id": existing_ids + [999999, 999998],  # include 2 new rows
            "order_id": np.random.choice(valid_order_ids, size=12, replace=True),
            "product_id": np.random.choice(valid_product_ids, size=12, replace=True),
            "return_ts": [datetime.now() for _ in range(12)],
            "qty": np.random.randint(10, 20, size=12),  # updated qty
            "reason": ["updated"] * 12,
            "return_reason_code": np.random.randint(1, 5, size=12).astype(float)
        })

        # print("Dataframe with upsert records", upsert_df)  # TESTING - TO GET LIST OF RECORDS TO BE UPSERTED
                
        upsert_ids = upsert_df["return_id"].tolist()

        dt.merge(
            upsert_df,
            predicate="source.return_id = target.return_id",
            source_alias="source",
            target_alias="target"
        ).when_matched_update_all().when_not_matched_insert_all().execute()

        print(f"🔁 UPSERT complete: updated {len(existing_ids)} rows, inserted 2 new rows")
        return upsert_ids


    # Step 8: Demonstrate DELETE
    def delete_returns(delta_path: pathlib.Path):
        dt = DeltaTable(str(delta_path))
        dt.delete("reason = 'no longer needed'")
        print("DELETE complete: removed rows with reason = 'no longer needed'")


    # Run UPSERT and DELETE
    upsert_returns(delta_path)
    # delete_returns(delta_path) ## TESTED.. commenting out delete to retain the records.

    # TESTING THE UPSERT RECORDS
    '''dt_final = DeltaTable(str(delta_path)) 
    final_df = dt_final.to_pandas()
    upsert_ids = upsert_returns(delta_path)

    upserted_df = final_df[final_df["return_id"].isin(upsert_ids)]
    print("Rows upserted in the last operation:")
    print(upserted_df)'''




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
    customer_count = int(80000)
    product_count = int(25000)    
    store_count = int(5000) 
    supplier_count = int(8000)
    orders_header_count = int(1000000)    
    orders_lines_count =  int(3000000)
    sensors_count = int(10000000)
    events_count = int(2000000)
    shipments_count = int(1000000)
    returns_count = int(100000)

    # sample row counts
    customer_sample = int(customer_count * args.scale)
    product_sample = int(product_count * args.scale)
    store_sample = int(store_count * args.scale)
    supplier_sample = int(supplier_count * args.scale)
    orders_header_sample = int(orders_header_count * args.scale)
    orders_lines_sample = int(orders_lines_count * args.scale)
    sensors_sample = int(sensors_count * args.scale)  
    events_sample = int(events_count * args.scale)
    shipments_sample = int(shipments_count * args.scale)
    returns_sample = int(returns_count * args.scale)

    # Print counts for verification
    print(customer_sample,"customer_sample", store_sample,"store_sample",product_sample,"product_sample", supplier_sample,"supplier_sample", orders_header_count,"orders_header_sample", orders_lines_count,"orders_lines_sample", sensors_count,"sensors_sample",events_count,"events_sample",shipments_count,"shipments_sample",     returns_sample,"returns_sample")

    # Generate scaled data into samples/
    print(datetime.now(),"Start of data generation")
    print_memory_usage("Start")
    generate_customers(samples_dir, customer_sample)
    generate_products(samples_dir, product_sample)
    generate_stores(samples_dir, store_sample)
    generate_suppliers(samples_dir, supplier_sample)
    generate_orders_header(samples_dir, orders_header_sample)
    generate_orders_lines(samples_dir, orders_lines_sample)
    generate_sensors(samples_dir, sensors_sample)
    generate_events(samples_dir, events_sample)
    generate_exchange_rates(samples_dir)
    generate_shipments(samples_dir, shipments_sample)  # smaller sample for shipments
    generate_returns(samples_dir, returns_sample)
    
 
    # Generate full data into out/
    '''
    generate_customers(out,customer_count)
    generate_products(out,product_count)
    generate_stores(out,store_count)
    generate_suppliers(out,supplier_count)
    generate_orders_header(out,orders_header_count)
    generate_orders_lines(out,orders_lines_count)
    generate_sensors(out,sensors_count)
    generate_events(out,events_count)
    generate_exchange_rates(out)
    generate_shipments(out,shipments_count)
    generate_returns(out,returns_count)
    '''


 
    print(datetime.now(),"End of data generation")
    print_memory_usage("End")

    print("All data generation complete.")
    
if __name__ == '__main__':
    main()
