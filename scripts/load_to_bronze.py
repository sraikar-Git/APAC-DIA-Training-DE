import argparse, pathlib, hashlib, datetime as dt, os, json
import duckdb
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import pyarrow.dataset as pads
from deltalake import write_deltalake
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from schemas import (
    customers_schema, products_schema, stores_schema, suppliers_schema,
    orders_header_schema, orders_lines_schema, sensors_schema, events_schema,
    exchange_rates_schema, shipments_schema, returns_day1_schema
)
from pyarrow import concat_tables

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

def already_processed(conn, key):
    row = conn.execute(
        "SELECT status FROM manifest_processed_files WHERE src_path = ?", [key]
    ).fetchone()
    if row is None:
        return False
    return row[0] == "SUCCESS"

def mark_processed(conn, key, row_count, reject_count=0, status="SUCCESS"):
    conn.execute(
        "INSERT OR REPLACE INTO manifest_processed_files VALUES (?, ?, ?, ?, ?)",
        [key, dt.datetime.now(dt.UTC), row_count, reject_count, status]
    )

# --------------------------
# Manifest Key Helper
# --------------------------
'''
def make_manifest_key(path: pathlib.Path, raw_root: pathlib.Path) -> str:
    """
    Return a relative path starting from raw_root folder name.
    Example: data_raw/samples/customers.csv
    """
    return str(path.relative_to(pathlib.Path.cwd())).replace("\\", "/")  '''


def make_manifest_key(path: pathlib.Path, raw_root: pathlib.Path) -> str:
    """
    Always store manifest key starting with 'data_raw/...'
    regardless of whether --raw points to data_raw or a subfolder like data_raw/samples.
    """
    path = path.resolve()
    raw_root = raw_root.resolve()

    # If user passed data_raw/samples, go one level up to data_raw
    if raw_root.name == "samples":
        raw_root = raw_root.parent

    return str(path.relative_to(raw_root.parent)).replace("\\", "/")


# --------------------------
# Logging Helper
# --------------------------
def log_processed(table_name, src, total_rows, total_rejects):
    print(f"✅ Processed {total_rows} rows ({total_rejects} rejects) from {src}")

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
    table = table.select([field.name for field in schema])
    table = table.cast(schema, safe=False)
    return table

# --------------------------
# Validation & Rejects
# --------------------------
def validate_and_split(table, schema, reject_path: pathlib.Path, table_name=None, output_format="parquet"):
    valid_rows, reject_rows, reason_codes = [], [], []

    for i in range(len(table)):
        row = table.slice(i, 1)
        try:
            row = row.cast(schema, safe=True)
            valid_rows.append(row)
        except Exception as e:
            reject_rows.append(row)
            reason_codes.append(str(e))

    valid_table = pa.concat_tables(valid_rows) if valid_rows else pa.Table.from_batches([], schema=schema)
    reject_count = 0

    if reject_rows:
        reject_path.mkdir(parents=True, exist_ok=True)

        if output_format == "jsonl":
            reject_dicts = []
            for row, reason in zip(reject_rows, reason_codes):
                if "json" in row.column_names:
                    raw_json = row.column("json")[0].as_py()
                    try:
                        obj = json.loads(raw_json)
                    except Exception:
                        obj = {"_raw": raw_json}
                    obj["reject_reason"] = reason
                    reject_dicts.append(obj)
                else:
                    reject_dicts.append({**row.to_pydict(), "reject_reason": reason})

            reject_file = reject_path / f"rejects_{dt.datetime.now().isoformat()}.jsonl"
            with open(reject_file, "w", encoding="utf-8") as f:
                for obj in reject_dicts:
                    f.write(json.dumps(obj) + "\n")

        else:
            reject_table = pa.concat_tables(reject_rows)
            reject_table = reject_table.append_column("reject_reason", pa.array(reason_codes))
            pq.write_table(reject_table, reject_path / f"rejects_{dt.datetime.now().isoformat()}.parquet")

        reject_count = len(reject_rows)

    return valid_table, reject_count

# --------------------------
# Write Helpers
# --------------------------
def write_parquet_partitioned(table, base_path, partitioning=None):
    import pyarrow.dataset as ds
    if partitioning:
        partitioning = ds.partitioning(
            schema=pa.schema([table.schema.field(col) for col in partitioning]),
            flavor="hive"
        )
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
# Path Helper for Sample Handling
# --------------------------
def _get_base_path(raw_root, is_sample):
    base_path = raw_root
    if is_sample and base_path.name != "samples":
        base_path = base_path / "samples"
    return base_path

# --------------------------
# Loaders for Single-file Tables
# --------------------------
def load_table(table_name, schema, raw_root, lake_root, conn, is_sample=False):
    base_path = _get_base_path(raw_root, is_sample)
    src = base_path / f"{table_name}.csv"
    manifest_key = make_manifest_key(src, raw_root)
    print( manifest_key,"CSV PATH")
   
    if already_processed(conn, manifest_key):
        print(f"⏩ Skipping {table_name}, already processed successfully.")
        return
    try:
        raw_table = pacsv.read_csv(src, read_options=pacsv.ReadOptions(encoding='utf-8'))
        process_and_write(table_name, raw_table, schema, src.name, src, lake_root, conn, manifest_key, is_sample)
    except Exception as e:
        print(f"❌ Failed processing {table_name}: {e}")
        mark_processed(conn, manifest_key, 0, 0, "FAIL")

def load_excel_table(table_name, schema, raw_root, lake_root, conn, is_sample=False):
    base_path = _get_base_path(raw_root, is_sample)
    src = base_path / f"{table_name}.xlsx"
    manifest_key = make_manifest_key(src, raw_root)
    print( manifest_key,"excel PATH")
    
    if already_processed(conn, manifest_key):
        print(f"⏩ Skipping {table_name}, already processed successfully.")
        return
    try:
        df = pd.read_excel(src, engine="openpyxl")
        raw_table = pa.Table.from_pandas(df, preserve_index=False)
        process_and_write(table_name, raw_table, schema, src.name, src, lake_root, conn, manifest_key, is_sample)
    except Exception as e:
        print(f"❌ Failed processing {table_name}: {e}")
        mark_processed(conn, manifest_key, 0, 0, "FAIL")

def load_parquet_table(table_name, schema, raw_root, lake_root, conn, is_sample=False):
    base_path = _get_base_path(raw_root, is_sample)
    src = base_path / f"{table_name}.parquet"
    manifest_key = make_manifest_key(src, raw_root)
    print( manifest_key,"parquet PATH")
    
    if already_processed(conn, manifest_key):
        print(f"⏩ Skipping {table_name}, already processed successfully.")
        return
    try:
        raw_table = pq.read_table(src)
        process_and_write(table_name, raw_table, schema, src.name, src, lake_root, conn, manifest_key, is_sample)
    except Exception as e:
        print(f"❌ Failed processing {table_name}: {e}")
        mark_processed(conn, manifest_key, 0, 0, "FAIL")

def load_delta_table_with_schema_evolution(table_name, schema, raw_root, lake_root, conn, is_sample=False):
    base_path = _get_base_path(raw_root, is_sample)
    src = base_path / table_name
    manifest_key = make_manifest_key(src, raw_root)
    print( manifest_key,"delta PATH")
    
    if already_processed(conn, manifest_key):
        print(f"⏩ Skipping {table_name}, already processed successfully.")
        return
    try:
        df = duckdb.sql(f"SELECT * FROM delta_scan('{src}')").to_df()
        raw_table = pa.Table.from_pandas(df, preserve_index=False)
        process_and_write(table_name, raw_table, schema, f"{table_name}_delta", src, lake_root, conn, manifest_key, is_sample, merge_schema=True)
    except Exception as e:
        print(f"❌ Failed processing {table_name}: {e}")
        mark_processed(conn, manifest_key, 0, 0, "FAIL")

# --------------------------
# Shared Process & Write
# --------------------------
def process_and_write(table_name, raw_table, schema, src_filename, src_path, lake_root, conn, manifest_key, is_sample=False, merge_schema=False):
    total_rows = 0
    total_rejects = 0
    status = "SUCCESS"
    try:
        raw_table = enforce_schema(raw_table, schema)
        reject_dir = lake_root / "_rejects" / table_name / ("samples" if is_sample else "")
        valid_table, reject_count = validate_and_split(raw_table, schema, reject_dir)

        now = pa.scalar(dt.datetime.now(dt.UTC), type=pa.timestamp('us'))
        valid_table = valid_table.append_column('ingestion_ts', pa.array([now.as_py()] * len(valid_table), type=pa.timestamp('us')))
        valid_table = valid_table.append_column('src_filename', pa.array([src_filename] * len(valid_table)))
        row_hashes = [hashlib.md5(str(r).encode()).hexdigest() for r in valid_table.to_pylist()]
        valid_table = valid_table.append_column('src_row_hash', pa.array(row_hashes))

        pq_base = lake_root / 'bronze' / 'parquet' / table_name / ('samples' if is_sample else '')
        dl_base = lake_root / 'bronze' / 'delta' / table_name / ('samples' if is_sample else '')
        pq_base.mkdir(parents=True, exist_ok=True)
        dl_base.mkdir(parents=True, exist_ok=True)

        write_parquet_partitioned(valid_table, pq_base)
        write_delta(valid_table, dl_base, mode='append', merge_schema=merge_schema)
        if not is_sample:
            verify_outputs(pq_base, dl_base)

        total_rows = len(valid_table)
        total_rejects = reject_count
        log_processed(table_name, src_path, total_rows, total_rejects)
    except Exception as e:
        print(f"❌ Failed processing {table_name}: {e}")
        status = "FAIL"
    mark_processed(conn, manifest_key, total_rows, total_rejects, status)

# --------------------------
# Partitioned Table Loader
# --------------------------
def load_partitioned_table_parallel(table_name, schema, partition_cols, raw_root, lake_root, conn, is_sample=False):
    if isinstance(partition_cols, str):
        partition_cols = [partition_cols]
    base_path = _get_base_path(raw_root, is_sample)
    base_dir = base_path / table_name
    manifest_key = make_manifest_key(base_dir, raw_root)
    if not base_dir.exists():
        return
    print( manifest_key,"partitione path")
    
    if already_processed(conn, manifest_key):
        print(f"⏩ Skipping {table_name}, already processed successfully.")
        return

    data_files = []
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".csv") or file.endswith(".jsonl") or file.endswith(".json"):
                folder_path = pathlib.Path(root)
                data_files.append((folder_path, pathlib.Path(root) / file))
    if not data_files:
        return

    total_rows = 0
    total_rejects = 0
    status = "SUCCESS"
    tables_to_write = []

    def process_file(folder_path, data_file):
        nonlocal status
        try:
            reject_rows = []
            reason_codes = []
            reject_dir = lake_root / "_rejects" / table_name / ("samples" if is_sample else "")
            output_fmt = "jsonl" if table_name == "events" else "parquet"

            if data_file.suffix.lower() == ".csv":
                raw_table = pacsv.read_csv(data_file, read_options=pacsv.ReadOptions(encoding='utf-8'))
            elif data_file.suffix.lower() in [".jsonl", ".json"]:
                parsed_rows = []
                with open(data_file, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            obj = json.loads(line)
                            parsed_rows.append(obj)
                        except Exception as e:
                            reject_rows.append(line)
                            reason_codes.append(f"JSON parse error: {str(e)}")
                if reject_rows:
                    relative_path = data_file.relative_to(raw_root)
                    reject_file = lake_root / "_rejects" / relative_path
                    reject_file.parent.mkdir(parents=True, exist_ok=True)
                    with open(reject_file, "w", encoding="utf-8") as f:
                        for line, reason in zip(reject_rows, reason_codes):
                            try:
                                obj = json.loads(line)
                            except Exception:
                                obj = {"_raw": line}
                            obj["reject_reason"] = reason
                            f.write(json.dumps(obj) + "\n")
                if parsed_rows:
                    df = pd.DataFrame(parsed_rows)
                    raw_table = pa.Table.from_pandas(df, preserve_index=False)
                else:
                    return None, len(reject_rows)
            else:
                return None, 0

            for col in partition_cols:
                if col not in raw_table.column_names:
                    for part in folder_path.parts:
                        if part.startswith(f"{col}="):
                            val = part.split("=")[1]
                            raw_table = raw_table.append_column(col, pa.array([val] * len(raw_table), type=pa.string()))
                            break

            partition_data = {}
            for col in partition_cols:
                if col not in [f.name for f in schema]:
                    partition_data[col] = raw_table.column(col)
                    raw_table = raw_table.drop([col])

            raw_table = enforce_schema(raw_table, schema)
            valid_table, reject_count = validate_and_split(raw_table, schema, reject_dir, table_name, output_format=output_fmt)

            for col, data in partition_data.items():
                if col not in valid_table.column_names:
                    valid_table = valid_table.append_column(col, data)

            now = pa.scalar(dt.datetime.now(dt.UTC), type=pa.timestamp('us'))
            valid_table = valid_table.append_column('ingestion_ts', pa.array([now.as_py()] * len(valid_table), type=pa.timestamp('us')))
            valid_table = valid_table.append_column('src_filename', pa.array([data_file.name] * len(valid_table)))
            row_hashes = [hashlib.md5(str(r).encode()).hexdigest() for r in valid_table.to_pylist()]
            valid_table = valid_table.append_column('src_row_hash', pa.array(row_hashes))

            return valid_table, reject_count + len(reject_rows)
        except Exception as e:
            print(f"❌ Failed processing {data_file}: {e}")
            status = "FAIL"
            return None, 0

    max_threads = min(32, (os.cpu_count() or 1) * 4, len(data_files))
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        for table, rejects in executor.map(lambda args: process_file(*args), data_files):
            if table is not None:
                tables_to_write.append(table)
                total_rows += len(table)
                total_rejects += rejects

    if tables_to_write:
        final_table = concat_tables(tables_to_write)
        pq_base = lake_root / 'bronze' / 'parquet' / table_name / ('samples' if is_sample else '')
        dl_base = lake_root / 'bronze' / 'delta' / table_name / ('samples' if is_sample else '')
        pq_base.mkdir(parents=True, exist_ok=True)
        dl_base.mkdir(parents=True, exist_ok=True)
        write_parquet_partitioned(final_table, pq_base, partitioning=partition_cols)
        write_delta(final_table, dl_base, mode='append', partition_by=partition_cols)
        if not is_sample:
            verify_outputs(pq_base, dl_base)
        log_processed(table_name, base_dir, total_rows, total_rejects)

    mark_processed(conn, manifest_key, total_rows, total_rejects, status)

# --------------------------
# Main
# --------------------------
def main():
    args = parse_args()
    raw_root = pathlib.Path(args.raw).resolve()
    lake_root = pathlib.Path(args.lake)
    ensure_dirs(lake_root)
    pathlib.Path(args.manifest).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(args.manifest)
    conn.execute("INSTALL delta; LOAD delta;")
    init_manifest(conn)

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

    load_excel_table("exchange_rates", exchange_rates_schema, raw_root, lake_root, conn, args.sample)
    load_parquet_table("shipments", shipments_schema, raw_root, lake_root, conn, args.sample)
    load_delta_table_with_schema_evolution("returns", returns_day1_schema, raw_root, lake_root, conn, args.sample)

    partitioned_tables = [
        ("orders_header", orders_header_schema, "order_dt_local"),
        ("orders_lines", orders_lines_schema, "order_dt"),
        ("sensors", sensors_schema, ["store_id", "month"]),
        ("events", events_schema, "event_date")
    ]
    with ThreadPoolExecutor(max_workers=len(partitioned_tables)) as executor:
        futures = [executor.submit(load_partitioned_table_parallel, name, schema, part_col, raw_root, lake_root, conn, args.sample) for name, schema, part_col in partitioned_tables]
        for f in as_completed(futures):
            f.result()

    print("🏁 Bronze load completed in parallel for all tables.")

if __name__ == '__main__':
    main()
# --------------------------