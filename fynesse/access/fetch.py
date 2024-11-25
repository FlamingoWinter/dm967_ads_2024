import io
import os
import zipfile
from typing import Literal

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
) -> pd.DataFrame:
    url_by_level = {
            'oa'  : "https://open-geography-portalx-ons.hub.arcgis.com/api/download/v1/items/6beafcfd9b9c4c9993a06b6b199d7e6d/csv?layers=0",
            'msoa': "https://open-geography-portalx-ons.hub.arcgis.com/api/download/v1/items/61ff711e89ba4c24ae5dc8a487e422a8/csv?layers=0"
    }
    url = url_by_level[level]

    extract_dir = os.path.join(DATA_DIRECTORY, "census2021-geography")
    path = os.path.join(extract_dir, f"census2021-{level}.csv")

    if not (os.path.exists(path)):
        os.makedirs(extract_dir, exist_ok=True)
        response = requests.get(url)
        response.raise_for_status()
        with open(path, 'wb') as f:
            f.write(response.content)

    return pd.read_csv(path)
