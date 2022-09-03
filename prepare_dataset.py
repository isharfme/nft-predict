#!/usr/bin/env python3

from genericpath import exists
import os
import csv
import requests
import json
from urllib.parse import urljoin
from PIL import Image, ImageStat, UnidentifiedImageError
from io import BytesIO

OPENSEA_API_KEY=str(os.getenv("OPENSEA_API_KEY"))
OPENSEA_BASE_URL=str(os.getenv("OPENSEA_URL"))

_ASSETS_OUTPUT_FILE="assets.csv"
EVENTS_FILE="events.csv"
DATASET_FILE="training.csv"

def prepare_dataset():
    print("Preapring NFT dataset...")

    with open(EVENTS_FILE) as infile:
        event_reader = csv.reader(infile)
        events_header = next(event_reader)

        with open(DATASET_FILE, "w") as outfile:
            dataset_writer = csv.writer(outfile)

            dataset_row_count = 0

            write_header = True
            for event in event_reader:
                # skip malformed JSON
                try:
                    asset = json.loads(event[0])
                except json.decoder.JSONDecodeError:
                    continue

                # append only data of following formats
                for ext in ["png", "gif", "jpeg"]:
                    if exists(f"assets/{asset['id']}.{ext}"):
                        image = Image.open(f"assets/{asset['id']}.{ext}")
                        break
                else:
                    continue

                # payment token defaults
                payment_token = {
                    "symbol": "", 
                    "decimals": 0, 
                    "eth_price": 0, 
                    "usd_price": 0
                }
                
                if event[6]:
                    payment_token = json.loads(event[6])

                if image.mode != "RGB" and image.mode != "RGBA" and image.mode != "P":
                    print(f"Warning: {asset['id']} mode is {image.mode}")

                # image_bands = image.getbands()
                # if image_bands[0] != 'R' or image_bands[1] != 'G' or image_bands[2] != 'B':
                #     print(f"Warning: {asset['id']} bands is {image_bands}")

                # if not image.info:
                #     print(f"Warning: {asset['id']} info is {image.info}")

                # image_colors = image.getcolors()
                # if image_colors is not None:
                #     print(f"Warning: {asset['id']} colors are {image_colors}")

                # image_palette = image.getpalette()
                # if image_palette is not None:
                #     print(f"Warning: {asset['id']} palette is {image_palette}")

                image_stat = ImageStat.Stat(image)
                
                image_extrema = [None] * 8
                i = 0

                image_extrema_v = image.getextrema()

                for v in image.getextrema():
                    if isinstance(v, tuple):
                        image_extrema[i] = v[0]
                        image_extrema[i+1] = v[1]
                        i = i + 2
                    else:
                        image_extrema[i] = v
                        i = i + 1

                dataset_row = {
                    "id": asset["id"],
                    "is_private": event[21],
                    "starting_price": event[24],
                    "ending_price": event[18],
                    "total_price": event[5],
                    "payment_symbol": payment_token["symbol"],
                    "payment_decimals": payment_token["decimals"],
                    "payment_eth_price": payment_token["eth_price"],
                    "payment_usd_price": payment_token["usd_price"],
                    "auction_type": event[4],
                    "quantity": event[9],
                    "asset_bundle": event[1],
                    "num_sales": asset["num_sales"],
                    "background_color": asset["background_color"],
                    "mode": image.mode,
                    "r_pixel_count": image_stat.count[0] if len(image_stat.count) > 0 else None,
                    "g_pixel_count": image_stat.count[1] if len(image_stat.count) > 1 else None,
                    "b_pixel_count": image_stat.count[2] if len(image_stat.count) > 2 else None,
                    "r_pixel_sum": image_stat.sum[0] if len(image_stat.sum) > 0 else None,
                    "g_pixel_sum": image_stat.sum[1] if len(image_stat.sum) > 1 else None,
                    "b_pixel_sum": image_stat.sum[2] if len(image_stat.sum) > 2 else None,
                    "r_pixel_sum2": image_stat.sum2[0] if len(image_stat.sum2) > 0 else None,
                    "g_pixel_sum2": image_stat.sum2[1] if len(image_stat.sum2) > 1 else None,
                    "b_pixel_sum2": image_stat.sum2[2] if len(image_stat.sum2) > 2 else None,
                    "r_pixel_mean": image_stat.mean[0] if len(image_stat.mean) > 0 else None,
                    "g_pixel_mean": image_stat.mean[1] if len(image_stat.mean) > 1 else None,
                    "b_pixel_mean": image_stat.mean[2] if len(image_stat.mean) > 2 else None,
                    "r_pixel_median": image_stat.median[0] if len(image_stat.median) > 0 else None,
                    "g_pixel_median": image_stat.median[1] if len(image_stat.median) > 1 else None,
                    "b_pixel_median": image_stat.median[2] if len(image_stat.median) > 2 else None,
                    "r_rms": image_stat.rms[0] if len(image_stat.rms) > 0 else None,
                    "g_rms": image_stat.rms[1] if len(image_stat.rms) > 1 else None,
                    "b_rms": image_stat.rms[2] if len(image_stat.rms) > 2 else None,
                    "r_var": image_stat.var[0] if len(image_stat.var) > 0 else None,
                    "g_var": image_stat.var[1] if len(image_stat.var) > 1 else None,
                    "b_var": image_stat.var[2] if len(image_stat.var) > 2 else None,
                    "r_stddev": image_stat.stddev[0] if len(image_stat.stddev) > 0 else None,
                    "g_stddev": image_stat.stddev[1] if len(image_stat.stddev) > 1 else None,
                    "b_stddev": image_stat.stddev[2] if len(image_stat.stddev) > 2 else None,
                    "r_pixel_min": image_extrema[0],
                    "r_pixel_max": image_extrema[1],
                    "g_pixel_min": image_extrema[2],
                    "g_pixel_max": image_extrema[3],
                    "b_pixel_min": image_extrema[4],
                    "b_pixel_max": image_extrema[5],
                    "a_pixel_min": image_extrema[6],
                    "a_pixel_max": image_extrema[7],
                    "entropy": image.entropy(),
                    "width": image.width,
                    "height": image.height,
                    "format": image.format,
                    "is_animated": 1 if getattr(image, "is_animated", False) else 0,
                    "n_frames": getattr(image, "n_frames", 1)
                }

                histogram_v = image.histogram()
                histogram = image.histogram()
                histogram = histogram + [0] * (1024-len(histogram))
                
                for (i,v) in enumerate(histogram):
                    dataset_row['histogram'+str(i)] = v

                if write_header:
                    keys = dataset_row.keys()
                    dataset_writer.writerow(list(keys))
                    write_header = False

                dataset_writer.writerow(list(dataset_row.values()))
                dataset_row_count = dataset_row_count + 1


                print(f"Event {event_reader.line_num}", end="\r")

            print(f"Crated {dataset_row_count} row(s) dataset")

if __name__ == "__main__":
    prepare_dataset()
    exit()