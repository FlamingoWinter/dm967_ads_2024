import random

import geopandas as gpd
import pandas as pd
from shapely import wkt

from fynesse.common.db import db


def run_query(conn, query):
    return db.run_query(conn, query)


def database_df_to_gpd(df, geometry_column_name="geometry"):
    df[geometry_column_name] = df[geometry_column_name].apply(
            lambda x: wkt.loads(x) if pd.notnull(x) else None
    )
    df = gpd.GeoDataFrame(df, geometry=geometry_column_name, crs="EPSG:27700")
    return df.to_crs("EPSG:4326", inplace=True)


def random_sample(connection, table_name, sample_number):
    count = run_query(connection, f"SELECT COUNT(*) FROM {table_name}").iloc[0, 0]
    sample_indexes = random.sample(range(count), sample_number)

    run_query(connection, f"DROP Table IF EXISTS sample_ids;")

    run_query(connection, "CREATE TEMPORARY TABLE sample_ids (id INT);")

    insert_query = "INSERT INTO sample_ids (id) VALUES (%s);"
    cursor = connection.cursor()
    cursor.executemany(insert_query, [(i,) for i in sample_indexes])

    sample = run_query(connection,
                       f"SELECT t.* FROM {table_name} t JOIN sample_ids s ON t.id = s.id;"
                       )

    run_query(connection, "DROP TABLE sample_ids;")

    return sample
