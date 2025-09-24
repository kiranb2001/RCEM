import csv
import json
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Go into the ClickHouse subdirectory
mapping_path = os.path.join(BASE_DIR, "ClickHouse", "mapping.json")

def load_mapping(mapping_file):
    """Load column mapping from JSON file"""
    with open(mapping_file, 'r', encoding='utf-8') as f:
        return json.load(f)

def compare_csv(file1, file2, output_file, mapping_file=None):
    """
    Compare two CSV files with column mapping defined in a JSON file.
    """
    column_mapping = load_mapping(mapping_file) if mapping_file else None

    with open(file1, newline='', encoding='utf-8') as f1, \
         open(file2, newline='', encoding='utf-8') as f2:

        reader1 = list(csv.DictReader(f1))
        reader2 = list(csv.DictReader(f2))

        if column_mapping is None:
            headers1 = reader1[0].keys() if reader1 else []
            column_mapping = {col: col for col in headers1}
        else:
            headers1 = column_mapping.keys()

        mismatch_rows = []
        for row1, row2 in zip(reader1, reader2):
            mismatch = {}
            for col1, col2 in column_mapping.items():
                val1 = row1.get(col1)
                val2 = row2.get(col2)
                if val1 != val2:
                    mismatch[col1] = f"{val1} | {val2}"
            if mismatch:
                mismatch_rows.append(mismatch)

        if mismatch_rows:
            with open(output_file, 'w', newline='', encoding='utf-8') as out:
                writer = csv.DictWriter(out, fieldnames=headers1)
                writer.writeheader()
                for row in mismatch_rows:
                    row_to_write = {col: row.get(col, "") for col in headers1}
                    writer.writerow(row_to_write)
            print(f"✅ Mismatched CSV created: {output_file}")
        else:
            print("✅ No mismatches found.")

# ✅ Use mapping_path here

if __name__ == "__main__":
    # Full paths to your files
    file1 = r"C:\Users\Kiran.Beesanakoppa\PycharmProjects\Kiran\NQS_DATA_VALIDATION\ClickHouse\ClickHouse_query_outputs\ClickHouse_nqs_denominator_clickhouse.csv"
    file2 = r"C:\Users\Kiran.Beesanakoppa\PycharmProjects\Kiran\NQS_DATA_VALIDATION\ClickHouse\ClickHouse_query_outputs\ClickHouse_tripmetrics_denominator_clickhouse.csv"
    output = r"C:\Users\Kiran.Beesanakoppa\PycharmProjects\Kiran\NQS_DATA_VALIDATION\ClickHouse\ClickHouse_query_outputs\mismatched.csv"

    compare_csv(file1, file2, output, mapping_path)