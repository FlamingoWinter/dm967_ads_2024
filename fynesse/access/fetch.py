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
        level: Literal['ctry', 'rgn', 'utla', 'ltla', 'msoa'] = 'msoa'
) -> pd.DataFrame:
    url = f'https://www.nomisweb.co.uk/output/census/2021/census2021-{code.lower()}.zip'
    extract_dir = os.path.join(DATA_DIRECTORY, os.path.splitext(os.path.basename(url))[0])

    if not (os.path.exists(extract_dir) and os.listdir(extract_dir)):
        os.makedirs(extract_dir, exist_ok=True)
        response = requests.get(url)
        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
            zip_ref.extractall(extract_dir)

    return pd.read_csv(f'{extract_dir}/census2021-{code.lower()}-{level}.csv')
