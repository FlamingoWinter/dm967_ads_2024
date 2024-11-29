import random

import geopandas as gpd
import pandas as pd
from pymysql import Connection
from shapely import wkt

from fynesse.common.db import db


def run_query(conn, query, args=None, execute_many=False):
    return db.run_query(conn, query, args, execute_many)


def database_df_to_gpd(df, geometry_column_name="geometry"):
    df[geometry_column_name] = df[geometry_column_name].apply(
            lambda x: wkt.loads(x) if pd.notnull(x) else None
    )
    df = gpd.GeoDataFrame(df, geometry=geometry_column_name, crs="EPSG:27700")
    df.to_crs("EPSG:4326", inplace=True)
    return df


def random_sample(connection, table_name, sample_number):
    count = run_query(connection, f"SELECT COUNT(*) FROM {table_name}").iloc[0, 0]
    sample_indexes = random.sample(range(count), sample_number)

    run_query(connection, f"DROP Table IF EXISTS sample_ids;")

    run_query(connection, "CREATE TEMPORARY TABLE sample_ids (id INT);")

    run_query(connection, "INSERT INTO sample_ids (id) VALUES (%s);",
              [(i,) for i in sample_indexes], execute_many=True
              )

    sample = run_query(connection,
                       f"SELECT t.* FROM {table_name} t JOIN sample_ids s ON t.id = s.id;"
                       )

    run_query(connection, "DROP TABLE sample_ids;")

    return sample


def abort_deletion_if_table_exists(connection: Connection, table_name: str) -> bool:
    return db.abort_deletion_if_table_exists(connection, table_name)
