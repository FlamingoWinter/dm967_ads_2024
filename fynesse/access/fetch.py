import io
import os
import time
import zipfile
from typing import Literal

import geopandas as gpd
import pandas as pd
import requests

DATA_DIRECTORY = "fetched_data"


# This was adapted from the example in practical 3
def fetch_2021_census_data(
        code: str,
        level: Literal['ctry', 'rgn', 'utla', 'ltla', 'msoa', 'oa'] = 'oa'
) -> pd.DataFrame:
    url = f'https://www.nomisweb.co.uk/output/census/2021/census2021-{code.lower()}.zip'
    extract_dir = os.path.join(DATA_DIRECTORY, os.path.splitext(os.path.basename(url))[0])

    if not (os.path.exists(extract_dir) and os.listdir(extract_dir)):
        os.makedirs(extract_dir, exist_ok=True)
        response = requests.get(url)
        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
            zip_ref.extractall(extract_dir)

    try:
        return pd.read_csv(f'{extract_dir}/census2021-{code.lower()}-{level}.csv')
    except FileNotFoundError:
        raise FileNotFoundError(
                f"File not found in download. Data may not exist for the {level} level"
        )


def fetch_2021_census_geography(
        level: Literal['msoa', 'oa'] = 'msoa'
) -> gpd.GeoDataFrame:
    url_by_level = {
            'oa'  : "https://open-geography-portalx-ons.hub.arcgis.com/api/download/v1/items/6beafcfd9b9c4c9993a06b6b199d7e6d/geojson?layers=0",
            'msoa': "https://open-geography-portalx-ons.hub.arcgis.com/api/download/v1/items/61ff711e89ba4c24ae5dc8a487e422a8/geojson?layers=0"
    }
    url = url_by_level[level]

    extract_dir = os.path.join(DATA_DIRECTORY, "census2021-geography")
    path = os.path.join(extract_dir, f"census2021-{level}.geojson")

    if not os.path.exists(path):
        pending = True

        response = None
        while pending:
            pending = False
            response = requests.get(url)
            response.raise_for_status()

            try:
                data = response.json()
                if data.get("status") == "Pending":
                    print("File not ready. Retrying.")
                    time.sleep(1)
                    pending = True
            except ValueError:
                pass

        os.makedirs(extract_dir, exist_ok=True)
        with open(path, 'wb') as f:
            f.write(response.content)

    gdf = gpd.read_file(path)
    gpd.read_file(path)
    gdf = gdf.set_geometry('geometry')

    return gdf


def fetch_uk_beaches():
    # from https://overpass-turbo.eu/
    url = "https://github.com/FlamingoWinter/ads_practicals/raw/refs/heads/main/uk-beaches.zip"
    zip_folder = "uk-beaches.zip"
    ext_folder = "beaches"

    if not os.path.exists(ext_folder):
        response = requests.get(url)
        with open(zip_folder, "wb") as file:
            file.write(response.content)

        os.makedirs(ext_folder, exist_ok=True)
        with zipfile.ZipFile(zip_folder, 'r') as zip_ref:
            zip_ref.extractall(ext_folder)

    for file in os.listdir(ext_folder):
        if file.endswith(".geojson"):
            geojson = os.path.join(ext_folder, file)
            break

    uk_beaches = gpd.read_file(geojson)

    uk_projected_beaches = uk_beaches.to_crs("EPSG:27700")

    uk_projected_beaches['centroid'] = uk_projected_beaches.geometry.centroid

    uk_beaches['centroid'] = uk_projected_beaches['centroid'].to_crs("EPSG:4326")

    uk_beaches['lat'] = uk_beaches['centroid'].y
    uk_beaches['lng'] = uk_beaches['centroid'].x
    uk_beaches.drop(columns=['centroid'], inplace=True)
