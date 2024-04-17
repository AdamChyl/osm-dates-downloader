import os
import csv
from collections import defaultdict
from datetime import datetime

def parse_timestamp(timestamp):
    try:
        return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
    except (ValueError, TypeError):
        return None

input_dir = "input"
output_dir = "output"

store_names = {
    "biedronka_output.csv": "biedronka",
    "delikatesy_centrum_output.csv": "delikatesy centrum",
    "dino_output.csv": "dino",
    "lewiatan_output.csv": "lewiatan",
    "lidl_output.csv": "lidl",
    "stokrotka_output.csv": "stokrotka",
    "zabka_output.csv": "Å»abka"
}

def clean_file(input_file, output_file, store_name):
    earliest_store_timestamps = defaultdict(lambda: datetime.max)
    store_found = set()

    with open(input_file, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter='\t')
        for row in reader:
            try:
                id_ = row['id']
                name = row['name']
                if name is not None:
                    name = name.lower() if store_name != "Å»abka" else name
                timestamp = parse_timestamp(row['timestamp'])
                
                if name == store_name and id_ not in store_found:
                    earliest_store_timestamps[id_] = timestamp
                    store_found.add(id_)
                elif name == store_name and timestamp < earliest_store_timestamps[id_]:
                    earliest_store_timestamps[id_] = timestamp
                    
            except KeyError:
                print("Missing key in row:", row)

    with open(input_file, newline='', encoding='utf-8') as csvfile, open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(csvfile, delimiter='\t')
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames, delimiter='\t')
        writer.writeheader()
        for row in reader:
            try:
                id_ = row['id']
                name = row['name']
                if name is not None:
                    name = name.lower() if store_name != "Å»abka" else name
                timestamp = parse_timestamp(row['timestamp'])
                
                if name == store_name and timestamp == earliest_store_timestamps[id_]:
                    writer.writerow(row)
            except KeyError:
                print("Missing key in row:", row)

os.makedirs(output_dir, exist_ok=True)

for filename in os.listdir(input_dir):
    if filename.endswith(".csv"):
        input_file = os.path.join(input_dir, filename)
        output_file = os.path.join(output_dir, filename.replace("_output.csv", "_node.csv"))
        store_name = store_names.get(filename, "Unknown Store")
        clean_file(input_file, output_file, store_name)
