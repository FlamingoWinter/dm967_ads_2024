import geopandas as gpd
import pandas as pd
from pandas.io.sql import get_schema
from pymysql import Connection

from ..db.db import abort_deletion_if_table_exists


def upload_to_database(conn: Connection, table_name: str, df: pd.DataFrame):
    if abort_deletion_if_table_exists(conn, table_name):
        return

    is_geodf = isinstance(df, gpd.GeoDataFrame)

    cursor = conn.cursor()

    schema = get_schema(df, table_name)
    create_table_query = (schema
                          .replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
                          .replace('"', '`'))

    if is_geodf:
        create_table_query = create_table_query.replace(
                "`geometry` VARCHAR(255)", "`geometry` GEOMETRY"
        )

    cursor.execute(create_table_query)

    placeholders = ", ".join(["%s"] * len(df.columns))

    if is_geodf:
        df["geometry"] = df["geometry"].apply(lambda geom: geom.wkt if geom else None)

    columns = ', '.join([f"`{col}`" for col in df.columns])
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    cursor.executemany(insert_query, [tuple(row) for row in df.to_records(index=False)])

    conn.commit()


def join_tables(connection, table1, table2, on, joined_table_name):
    if abort_deletion_if_table_exists(connection, joined_table_name):
        return

    cursor = connection.cursor()

    cursor.execute(f"SHOW COLUMNS FROM {table1}")
    table1_columns = [row[0] for row in cursor.fetchall()]

    cursor.execute(f"SHOW COLUMNS FROM {table2}")
    table2_columns = [row[0] for row in cursor.fetchall()]

    table2_columns = [col for col in table2_columns if col != on and col not in table1_columns]

    joined_columns = f"t1.*, {', '.join([f't2.`{col}`' for col in table2_columns])}"

    join_query = f"""
      CREATE TABLE {joined_table_name} AS
      SELECT {joined_columns}
      FROM {table1} t1
      JOIN {table2} t2 ON t1.{on} = t2.{on};
  """

    print(join_query)

    cursor = connection.cursor()

    cursor.execute(join_query)

    connection.commit()
