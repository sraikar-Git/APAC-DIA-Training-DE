import argparse, pathlib, hashlib, datetime as dt
import duckdb
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import pyarrow.dataset as pads
from deltalake import write_deltalake
from concurrent.futures import ThreadPoolExecutor, as_completed
from schemas import (
    customers_schema, products_schema, stores_schema, suppliers_schema, orders_header_schema, orders_lines_schema
)
from pyarrow import concat_tables
import os

# --------------------------
# Parse Arguments
# --------------------------
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument('--raw', type=str, default='data_raw')
    ap.add_argument('--lake', type=str, default='lake')
    ap.add_argument('--manifest', type=str, default='duckdb/warehouse.duckdb')
    ap.add_argument('--sample', action='store_true', help='Ingest sample data instead of full data')
    return ap.parse_args()

# --------------------------
# Ensure Directories
# --------------------------
def ensure_dirs(lake_root):
    for sub in ['bronze/parquet', 'bronze/delta', '_rejects']:
        (lake_root / sub).mkdir(parents=True, exist_ok=True)

# --------------------------
# Manifest Table
# --------------------------
def init_manifest(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS manifest_processed_files (
            src_path TEXT PRIMARY KEY,
            processed_at TIMESTAMP,
            row_count BIGINT,
            reject_count BIGINT,
            status TEXT
        )
    ''')

def already_processed(conn, p):
    """Skip only if SUCCESS, reprocess if FAIL or not present."""
    row = conn.execute(
        "SELECT status FROM manifest_processed_files WHERE src_path = ?", [str(p)]
    ).fetchone()
    if row is None:
        return False
    return row[0] == "SUCCESS"

def mark_processed(conn, p, row_count, reject_count=0, status="SUCCESS"):
    conn.execute(
        "INSERT OR REPLACE INTO manifest_processed_files VALUES (?, ?, ?, ?, ?)",
        [str(p), dt.datetime.now(dt.UTC), row_count, reject_count, status]
    )

# --------------------------
# Schema Enforcement
# --------------------------
def enforce_schema(table: pa.Table, schema: pa.schema) -> pa.Table:
    for field in schema:
        if field.name not in table.column_names:
            table = table.append_column(
                field.name,
                pa.array([None] * len(table), type=field.type)
            )
    # Keep only schema columns in correct order
    table = table.select([field.name for field in schema])
    table = table.cast(schema, safe=False)
    return table

# --------------------------
# Validation & Rejects
# --------------------------
def validate_and_split(table, schema, reject_path):
    valid_rows, reject_rows, reason_codes = [], [], []
    for i in range(len(table)):
        row = table.slice(i, 1)
        try:
            row.cast(schema, safe=True)
            valid_rows.append(row)
        except pa.lib.ArrowInvalid as e:
            reject_rows.append(row)
            reason_codes.append(str(e))
    valid_table = pa.concat_tables(valid_rows) if valid_rows else pa.Table.from_batches([], schema=schema)
    reject_count = 0
    if reject_rows:
        reject_table = pa.concat_tables(reject_rows)
        reject_table = reject_table.append_column("reject_reason", pa.array(reason_codes))
        reject_path.mkdir(parents=True, exist_ok=True)
        pq.write_table(reject_table, reject_path / f"rejects_{dt.datetime.now().isoformat()}.parquet")
        reject_count = len(reject_table)
    return valid_table, reject_count

# --------------------------
# Write Helpers
# --------------------------
def write_parquet_partitioned(table, base_path, partitioning=None):
    pads.write_dataset(
        table,
        base_dir=str(base_path),
        format='parquet',
        partitioning=partitioning,
        existing_data_behavior='overwrite_or_ignore'
    )

def write_delta(table, base_path, mode='append', partition_by=None, merge_schema=False):
    kwargs = {"mode": mode, "partition_by": partition_by or []}
    if merge_schema:
        kwargs["schema_mode"] = "merge"
    write_deltalake(str(base_path), table, **kwargs)

def verify_outputs(pq_path, delta_path):
    pq_count = sum(pq.read_table(str(f)).num_rows for f in pathlib.Path(pq_path).rglob("*.parquet"))
    delta_count = duckdb.sql(f"SELECT COUNT(*) FROM delta_scan('{delta_path}')").fetchone()[0]
    if pq_count != delta_count:
        raise ValueError(f"Row count mismatch: Parquet={pq_count}, Delta={delta_count}")

# --------------------------
# Loader for Flat CSV Tables
# --------------------------
def load_table(table_name, schema, raw_root, lake_root, conn, is_sample=False):
    src = raw_root / f"{table_name}.csv"
    manifest_key = str(src)
    if already_processed(conn, manifest_key):
        print(f"⏩ Skipping {src}, already processed successfully.")
        return

    total_rows = 0
    total_rejects = 0
    status = "SUCCESS"

    try:
        raw_table = pacsv.read_csv(src, read_options=pacsv.ReadOptions(encoding='utf-8'))
        raw_table = enforce_schema(raw_table, schema)
        valid_table, reject_count = validate_and_split(raw_table, schema, lake_root / "_rejects" / table_name)

        now = pa.scalar(dt.datetime.now(dt.UTC), type=pa.timestamp('us'))
        valid_table = valid_table.append_column('ingestion_ts', pa.array([now.as_py()] * len(valid_table), type=pa.timestamp('us')))
        valid_table = valid_table.append_column('src_filename', pa.array([src.name] * len(valid_table)))
        row_hashes = [hashlib.md5(str(r).encode()).hexdigest() for r in valid_table.to_pylist()]
        valid_table = valid_table.append_column('src_row_hash', pa.array(row_hashes))

        pq_base = lake_root / 'bronze' / 'parquet' / table_name / ('samples' if is_sample else '')
        dl_base = lake_root / 'bronze' / 'delta' / table_name / ('samples' if is_sample else '')
        pq_base.mkdir(parents=True, exist_ok=True)
        dl_base.mkdir(parents=True, exist_ok=True)

        write_parquet_partitioned(valid_table, pq_base)
        write_delta(valid_table, dl_base, mode='append')

        if not is_sample:
            verify_outputs(pq_base, dl_base)

        total_rows = len(valid_table)
        total_rejects = reject_count
        print(f"✅ Processed {len(valid_table)} rows ({reject_count} rejects) from {src}")

    except Exception as e:
        print(f"❌ Failed processing {table_name}: {e}")
        status = "FAIL"

    mark_processed(conn, manifest_key, total_rows, total_rejects, status)
    print(f"📦 Manifest updated for {table_name} with status {status}")

# --------------------------
# Loader for Partitioned Tables
# --------------------------
def load_partitioned_table_parallel(table_name, schema, partition_col, raw_root, lake_root, conn, is_sample=False):
    base_dir = raw_root / table_name
    if not base_dir.exists():
        print(f"⚠️ No directory found at {base_dir}, skipping.")
        return

    manifest_key = str(base_dir)
    if already_processed(conn, manifest_key):
        print(f"⏩ Skipping {table_name}, already processed successfully.")
        return

    # Gather all CSV files from all partitions
    csv_files = [
        (date_folder, csv_file)
        for date_folder in base_dir.iterdir() if date_folder.is_dir()
        for csv_file in date_folder.glob("*.csv")
    ]

    total_rows = 0
    total_rejects = 0
    status = "SUCCESS"
    tables_to_write = []

    def process_csv(date_folder, csv_file):
        nonlocal status
        # Per-file skip check (in-memory only)
        # If you want to skip based on manifest, you could query here with file path
        try:
            raw_table = pacsv.read_csv(csv_file, read_options=pacsv.ReadOptions(encoding='utf-8'))

            # Add partition col only if missing
            if partition_col not in raw_table.column_names:
                part_array = pa.array([date_folder.name] * len(raw_table), type=pa.string())
                raw_table = raw_table.append_column(partition_col, part_array)

            # Save partition col separately if not in schema
            partition_data = None
            if partition_col not in [f.name for f in schema]:
                partition_data = raw_table.column(partition_col)
                raw_table = raw_table.drop([partition_col])

            raw_table = enforce_schema(raw_table, schema)
            valid_table, reject_count = validate_and_split(raw_table, schema, lake_root / "_rejects" / table_name)

            # Reattach partition col for writing
            if partition_data is not None and partition_col not in valid_table.column_names:
                valid_table = valid_table.append_column(partition_col, partition_data)

            # Add audit columns
            now = pa.scalar(dt.datetime.now(dt.UTC), type=pa.timestamp('us'))
            valid_table = valid_table.append_column('ingestion_ts', pa.array([now.as_py()] * len(valid_table), type=pa.timestamp('us')))
            valid_table = valid_table.append_column('src_filename', pa.array([csv_file.name] * len(valid_table)))
            row_hashes = [hashlib.md5(str(r).encode()).hexdigest() for r in valid_table.to_pylist()]
            valid_table = valid_table.append_column('src_row_hash', pa.array(row_hashes))

            return valid_table, reject_count

        except Exception as e:
            print(f"❌ Failed processing {csv_file}: {e}")
            status = "FAIL"
            return None, 0

    # Dynamic thread pool size
    max_threads = min(32, (os.cpu_count() or 1) * 4, len(csv_files))

    # Parallel processing
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        for table, rejects in executor.map(lambda args: process_csv(*args), csv_files):
            if table is not None:
                tables_to_write.append(table)
                total_rows += len(table)
                total_rejects += rejects

    # Single write to Parquet and Delta
    if tables_to_write:
        final_table = concat_tables(tables_to_write)
        pq_base = lake_root / 'bronze' / 'parquet' / table_name / ('samples' if is_sample else '')
        dl_base = lake_root / 'bronze' / 'delta' / table_name / ('samples' if is_sample else '')
        pq_base.mkdir(parents=True, exist_ok=True)
        dl_base.mkdir(parents=True, exist_ok=True)

        write_parquet_partitioned(final_table, pq_base, partitioning=[partition_col])
        write_delta(final_table, dl_base, mode='append', partition_by=[partition_col])

        if not is_sample:
            verify_outputs(pq_base, dl_base)

    # ONE manifest entry per table
    mark_processed(conn, manifest_key, total_rows, total_rejects, status)
    print(f"📦 Manifest updated for {table_name} with status {status}")

# --------------------------
# Main
# --------------------------
def main():
    args = parse_args()
    raw_root = pathlib.Path(args.raw)
    lake_root = pathlib.Path(args.lake)
    ensure_dirs(lake_root)
    pathlib.Path(args.manifest).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(args.manifest)
    conn.execute("INSTALL delta; LOAD delta;")
    init_manifest(conn)

    # Flat tables
    flat_tables = [
        ("customers", customers_schema),
        ("products", products_schema),
        ("stores", stores_schema),
        ("suppliers", suppliers_schema)
    ]
    with ThreadPoolExecutor(max_workers=len(flat_tables)) as executor:
        futures = [executor.submit(load_table, name, schema, raw_root, lake_root, conn, args.sample) for name, schema in flat_tables]
        for f in as_completed(futures):
            f.result()

    # Partitioned tables
    partitioned_tables = [
        ("orders_header", orders_header_schema, "order_dt_local"),
        ("order_lines", orders_lines_schema, "order_dt")
    ]
    with ThreadPoolExecutor(max_workers=len(partitioned_tables)) as executor:
        futures = [executor.submit(load_partitioned_table_parallel, name, schema, part_col, raw_root, lake_root, conn, args.sample) for name, schema, part_col in partitioned_tables]
        for f in as_completed(futures):
            f.result()

    print("🏁 Bronze load completed in parallel for all tables.")

if __name__ == '__main__':
    main()
