import requests
import json
import threading
import os
import csv
from tqdm import tqdm

input_directory = './sklepy/'
output_directory = './output/'

geojson_files = [
    'biedronka.geojson',
    'delikatesy_centrum.geojson',
    'dino.geojson',
    'lewiatan.geojson',
    'lidl.geojson',
    'stokrotka.geojson',
    'zabka.geojson'
]

total_brands = len(geojson_files)
max_threads = 10

thread_semaphore = threading.Semaphore(value=max_threads)

def fetch_and_save_data(geojson_file):
    global thread_semaphore
    with open(input_directory + geojson_file, encoding='utf-8') as file:
        source_data = json.load(file)

    total_features = len(source_data['features'])

    with tqdm(total=total_features, desc=f"Fetching data for {geojson_file}") as pbar:
        for feature in source_data['features']:
            fetch_data(feature, pbar, geojson_file)

def fetch_data(feature, pbar, geojson_file):
    global thread_semaphore
    try:
        with thread_semaphore:
            feature_id = feature['properties']['@id'].split('/')[1]
            mydata = """[out:csv(id, version,timestamp,changeset, name)];
            timeline(node,{});
            for (t["created"])
            {{
              retro(_.val)
              {{
                node({});
                make stat id=u(id()), version=u(version()),timestamp=u(timestamp()),changeset=u(changeset()),name=u(t["name"]);
                out;
              }}
            }}""".format(feature_id, feature_id)
            
            response = requests.post('https://overpass.kumi.systems/api/interpreter', data=mydata)
            response.raise_for_status()

            overpass_csv = response.text

            output_file = os.path.join(output_directory, geojson_file.replace('.geojson', '_output.csv'))
            with open(output_file, 'a', encoding='utf-8') as file:
                file.write(overpass_csv)

            pbar.update(1)
    except Exception as e:
        print(f"Error fetching data for {feature_id} of brand {geojson_file}: {e}")
        write_error(feature_id, geojson_file)

def write_error(feature_id, geojson_file):
    error_file = os.path.join(output_directory, 'errors.csv')
    with open(error_file, 'a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([feature_id, geojson_file])

def main():
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    error_file = os.path.join(output_directory, 'errors.csv')
    if not os.path.exists(error_file):
        with open(error_file, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['ID', 'Brand'])

    for geojson_file in geojson_files:
        print(f"Fetching and saving data for {geojson_file}...")
        fetch_and_save_data(geojson_file)
        print(f"Data for {geojson_file} fetched and saved.\n")

if __name__ == '__main__':
    main()
