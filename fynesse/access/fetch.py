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
