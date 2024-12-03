from random import sample

import geopandas as gpd

from fynesse.assess.query import database_df_to_gpd
from fynesse.common.db.db import run_query, abort_deletion_if_table_exists
from fynesse.common.db.db_operations import upload_to_database, append_to_database

progress_table_name = f"process_postcodes_progress"
index_table_name = f"process_postcodes_indexes"


def init_process_postcodes(connection):
    indexes = run_query(connection, f"SELECT id FROM postcode")
    indexes['random_order'] = sample(range(len(indexes)), len(indexes))
    upload_to_database(connection, index_table_name, indexes)

    if not abort_deletion_if_table_exists(connection, progress_table_name):
        create_query = f"""
                                CREATE TABLE {progress_table_name} (
                                id INTEGER PRIMARY KEY AUTO_INCREMENT, last_processed_index INTEGER NOT NULL
                            );
                            """
        run_query(connection, create_query)
        run_query(connection,
                  f"INSERT INTO {progress_table_name} (last_processed_index) VALUES (0)"
                  )

        run_query(connection, f"DROP TABLE IF EXISTS postcode_near_beach")


distance_km = 1
batch_size = 10000


def resume_process_postcodes(connection):
    beaches = run_query(connection, f"SELECT * FROM beach")
    beaches.rename(columns={'id': 'beach_id'}, inplace=True)

    beaches = database_df_to_gpd(beaches, crs="EPSG:4326")
    beaches = beaches.to_crs("EPSG:27700")

    beaches["geobuffer"] = beaches.geometry.buffer(distance_km * 1000)

    def postcodes_to_nearest_beach(postcodes):
        postcodes_near_beach = gpd.sjoin(
                postcodes, beaches.set_geometry("geobuffer"), how="inner",
                predicate="intersects"
        )

        postcodes_near_beach["distance_to_beach"] = postcodes_near_beach.apply(
                lambda row: row.geometry.distance(beaches.loc[row.index_right, "geometry"]), axis=1
        )

        nearest_beaches = (
                postcodes_near_beach.sort_values(by="distance_to_beach")  # Sort by distance
                .drop_duplicates(subset=postcodes.index.name)
        )
        return nearest_beaches[["postcode_id", "beach_id", "distance_to_beach"]]

    while True:
        last_processed_index = run_query(connection, f"SELECT * FROM {progress_table_name}")[
            "last_processed_index"]

        if len(last_processed_index) == 1:
            last_processed_index = last_processed_index[0]
        else:
            last_processed_index = 0

        batch_query = f"""
                    SELECT random_order, p.*
                    FROM {index_table_name} o
                    JOIN postcode p ON o.id = p.id
                    WHERE o.random_order > {last_processed_index}
                    ORDER BY o.random_order
                    LIMIT {batch_size}
                """
        postcodes = run_query(connection, batch_query)
        postcodes = gpd.GeoDataFrame(
                postcodes,
                geometry=gpd.points_from_xy(postcodes["longitude"], postcodes["latitude"]),
                crs="EPSG:4326"
        )
        postcodes = postcodes.to_crs("EPSG:27700")

        if len(postcodes) == 0:
            print(f"done with last processed index = {last_processed_index}")
            break

        postcodes = postcodes.rename(columns={"id": "postcode_id"})
        postcode_beach_distance = postcodes_to_nearest_beach(postcodes)

        append_to_database(connection, "postcode_near_beach", postcode_beach_distance)

        new_progress = postcodes['random_order'].max()
        run_query(connection,
                  f"UPDATE {progress_table_name} SET last_processed_index = {new_progress}"
                  )

        print(f"Processed up to random_order: {new_progress}...")

    print("Pipeline completed. Deleting metadata...")
    run_query(connection,
              f"DROP table IF EXISTS {progress_table_name}"
              )
    run_query(connection,
              f"DROP table IF EXISTS {index_table_name}"
              )
    print("Deleted Metadata")
