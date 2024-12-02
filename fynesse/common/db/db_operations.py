import geopandas as gpd
import pandas as pd
from pandas.io.sql import get_schema
from pymysql import Connection

from ..db.db import abort_deletion_if_table_exists, run_query


def upload_to_database(connection: Connection, table_name: str, df: pd.DataFrame,
                       temporary: bool = False
                       ) -> None:
    if abort_deletion_if_table_exists(connection, table_name):
        return

    is_gdf = isinstance(df, gpd.GeoDataFrame)

    table_type = "TEMPORARY" if temporary else ""
    create_table_query = (get_schema(df, table_name)
                          .replace("CREATE TABLE", f"CREATE {table_type} TABLE IF NOT EXISTS", 1)
                          .replace('"', '`'))

    if is_gdf:
        create_table_query = create_table_query.replace(
                "`geometry` VARCHAR(255)", "`geometry` GEOMETRY"
        )

    run_query(connection, create_table_query)

    values_placeholders = ", ".join(["%s"] * len(df.columns))
    columns = ', '.join([f"`{col}`" for col in df.columns])
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values_placeholders})"

    if is_gdf:
        df["geometry"] = df["geometry"].apply(lambda geometry: geometry.wkt if geometry else None)

    run_query(connection, insert_query, [tuple(record) for record in df.to_records(index=False)],
              execute_many=True
              )


def append_to_database(connection: Connection, table_name: str, df: pd.DataFrame) -> None:
    tables = run_query(connection, "SHOW Tables")

    if table_name not in tables[tables.columns[-1]].to_list():
        upload_to_database(connection, table_name, df)
    else:
        is_gdf = isinstance(df, gpd.GeoDataFrame)

        values_placeholders = ", ".join(["%s"] * len(df.columns))
        columns = ', '.join([f"`{col}`" for col in df.columns])
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values_placeholders})"

        if is_gdf:
            df["geometry"] = df["geometry"].apply(lambda geom: geom.wkt if geom else None)

        run_query(connection, insert_query, [tuple(row) for row in df.to_records(index=False)],
                  execute_many=True
                  )


def join_tables(connection: Connection, table1: str, table2: str, on: str, joined_table_name: str):
    if abort_deletion_if_table_exists(connection, joined_table_name):
        return

    table1_columns = run_query(connection, f"SHOW COLUMNS FROM {table1}")

    table2_columns = run_query(connection, f"SHOW COLUMNS FROM {table2}")

    table2_columns = [col for col in table2_columns if col != on and col not in table1_columns]

    joined_columns = f"t1.*, {', '.join([f't2.`{col}`' for col in table2_columns])}"

    run_query(connection, f"""
                                              CREATE TABLE {joined_table_name} AS
                                              SELECT {joined_columns}
                                              FROM {table1} t1
                                              JOIN {table2} t2 ON t1.{on} = t2.{on};
                                          """
              )


def join_in_place(connection, table_name, df_to_join, on):
    upload_to_database(connection, "temporary", df_to_join, temporary=True)

    schema = get_schema(df_to_join, "temporary").replace('"', '`')

    column_definitions = {
            line.split('`')[1]: line.split('`')[2].strip().split()[0].rstrip(",")
            for line in schema.split('\n') if '(' not in line and ')' not in line
    }

    for column_name, column_type in column_definitions.items():
        if column_name not in on:
            run_query(connection,
                      f"ALTER TABLE {table_name} ADD COLUMN `{column_name}` {column_type}"
                      )

    on_clause = " AND ".join([f"target.{col} = temp.{col}" for col in on])
    set_clause = ", ".join(
            [f"target.{col} = temp.{col}" for col in df_to_join.columns if col not in on]
    )

    run_query(connection, f"""
        UPDATE {table_name} AS target
        JOIN temporary AS temp
        ON {on_clause}
        SET {set_clause}
    """
              )

    run_query(connection, "DROP table temporary")
