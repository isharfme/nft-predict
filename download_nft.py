#!/usr/bin/env python3

import csv
import requests
import json
import sys
from urllib.parse import urljoin
from PIL import Image, ImageStat, UnidentifiedImageError
from io import BytesIO

OPENSEA_API_KEY="a4563dd083944ee2b6e9be567c27a805"
OPENSEA_BASE_URL="https://api.opensea.io/api/v1/"

MAX_ASSETS=1000
ASSETS_FILE="assets.csv"

MAX_EVENTS=1000
EVENTS_FILE="events.csv"

def opensea_query(endpoint, params, data_field_name, records_qty, direction, output_file):
    '''Query OpenSea API endpoint and write result into file in CSV format
    '''

    headers = {
        "Accept": "application/json",
        "X-API-KEY": OPENSEA_API_KEY
    }

    endpoint_url = urljoin(OPENSEA_BASE_URL, endpoint)

    csv_header_written = False
    with open(output_file, "w") as outfile:
        csv_writer = csv.writer(outfile)

        row_count = 0
        result = {}
        cursor = {}
        remaining_records = records_qty

        while True:
            response = requests.get(endpoint_url, params={**params, **cursor}, headers=headers)
            if response.ok:
                result = response.json()
                
                if not csv_header_written:
                    keys = result[data_field_name][0].keys()
                    csv_writer.writerow(list(keys))
                    csv_header_written = True
                

                for record in result[data_field_name]:
                    # we don't support bundled assets
                    if record['asset'] is None:
                        continue

                    row = []
                    for value in record.values():
                        if isinstance(value, dict):
                            row.append(json.dumps(value))
                        else:
                            row.append(value)

                    csv_writer.writerow(row)
                    row_count = row_count + 1

                remaining_records -= len(result[data_field_name])

                cursor = { "cursor": result[direction] }

                #print(f"Loaded {len(result[data_field_name])} {endpoint}, {direction} page is {cursor['cursor']}")

            if not response.ok or\
               remaining_records <= 0 or\
               cursor["cursor"] is None:
                break

        print(f"{row_count} {endpoint} rows written to {output_file}")


def fetch_assets():
    '''Get NFT assets from API and save to ASSETS_FILE
    '''

    # API responds with 20 records by default, 
    # may fetch up to 50 per query
    params = {
        "order_direction": "asc",
        "limit": 50,
        "include_orders": "false"
    }

    print(f"Querying assets...")
    opensea_query("assets", params, "assets", MAX_ASSETS, "previous", ASSETS_FILE)

def fetch_events():
    '''Get successful NFT sale events from API and save to EVENTS_FILE
    '''

    params = {
        "event_type": "successful"
    }

    print(f"Querying events...")
    opensea_query("events", params, "asset_events", MAX_EVENTS, "next", EVENTS_FILE)

def download_nft():
    '''Download NFT image files and store in 'assets' folder
    '''

    print(f"Downloading NFT files...")
    with open(EVENTS_FILE) as infile:
        event_reader = csv.reader(infile)
        next(event_reader)

        line_count = 0
        nft_count = 0
        for event in event_reader:
            print(f"Loaded {nft_count} NFTs", end="\r")

            try:
                asset = json.loads(event[0])
                line_count = line_count + 1
            except json.decoder.JSONDecodeError:
                print(f"JSON decode error at line {event_reader.line_num}", file=sys.stderr)
                continue

            try:
                response = requests.get(asset['image_url'])
                image = Image.open(BytesIO(response.content))
                image.save(f"assets/{asset['id']}.{image.format.lower()}")
                nft_count = nft_count + 1
            except Exception as e:
                continue
    print(f"Loaded {nft_count} NFTs")

if __name__ == "__main__":
    fetch_events()
    #fetch_assets()
    download_nft()
    exit()