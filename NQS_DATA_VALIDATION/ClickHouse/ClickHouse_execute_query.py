import os
import sys
import csv
import sqlparse

# absolute path to the directory containing dbconnectors.py
current_file = os.path.abspath(__file__)
base_dir = os.path.dirname(os.path.dirname(current_file))
sys.path.append(base_dir)

from dbconnectors import load_properties, connect_clickhouse

def get_sql_files(directory):
    return [f for f in os.listdir(directory) if f.endswith('.sql')]

def read_sql_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()

def write_csv(path, columns, rows):
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)

def write_error_csv(path, message):
    write_csv(path, ['error'], [[message]])

def split_statements(sql):
    sql = sql.strip()
    if not sql:
        return []
    return [s for s in sqlparse.split(sql) if s.strip()]

def execute_clickhouse(client, sql, out_csv):
    try:
        res = client.query(sql)
        cols = list(res.column_names) if getattr(res, 'column_names', None) else []
        rows = [tuple(r) for r in getattr(res, 'result_rows', [])]
        if not cols:
            write_error_csv(out_csv, 'ClickHouse returned no columns')
            return
        write_csv(out_csv, cols, rows)
        print(f"✅ ClickHouse output: {out_csv} (rows: {len(rows)})")
    except Exception as e:
        write_error_csv(out_csv, f"ClickHouse error: {e}")
        print(f"❌ ClickHouse error for {out_csv}: {e}")


def main():
    directory = os.path.dirname(os.path.abspath(__file__))
    sql_files = get_sql_files(directory)
    if not sql_files:
        print('No .sql files found in directory.')
        return
    parent = os.path.dirname(directory)
    config_path = os.path.join(parent, 'db.properties')
    config = load_properties(config_path)
    clickhouse = connect_clickhouse(config)
    out_dir = os.path.join(directory, 'ClickHouse_query_outputs')
    os.makedirs(out_dir, exist_ok=True)

    for sql_file in sql_files:
        path = os.path.join(directory, sql_file)
        sql = read_sql_file(path)
        base = os.path.splitext(sql_file)[0]

        # ClickHouse only
        if clickhouse:
            out_csv = os.path.join(out_dir, f"{base}_clickhouse.csv")
            execute_clickhouse(clickhouse, sql, out_csv)


if __name__ == '__main__':
    main()
