from random import sample

import osmnx as ox
import pandas as pd

from fynesse.common.db.db import run_query, abort_deletion_if_table_exists
from fynesse.common.db.db_operations import upload_to_database, append_to_database


def amend_df_with_indicators(df,
                             distance_km: float = 1.0,
                             lat_column_name="lat",
                             lng_column_name="lng",
                             geometry_column_name="geometry",
                             radius_around_point=False
                             ):
    def count_pois(row):
        tags = {
                "building"      : ["dormitory", "university", "academic", "office", "residential"],
                "leisure"       : ["playground", "park"],
                "amenity"       : ["bench", "fast_food", "school", "nightclub", "pub", "cafe",
                                   "theatre", "cinema"],
                "residential"   : ["retirement_home"],
                "shop"          : ["books", "clothes"],
                "toilets:unisex": ["yes"]
        }

        columns = {
                "houses"              : {"building": "residential"},

                "dorms"               : {"building": "dormitory"},
                "university_buildings": {"building": "university"},
                "academic_buildings"  : {"building": "academic"},
                "office_buildings"    : {"building": "office"},

                "playgrounds"         : {"leisure": "playground"},
                "benches"             : {"amenity": "bench"},
                "retirement_homes"    : {"residential": "retirement_home"},
                "fast_foods"          : {"amenity": "fast_food"},
                "schools"             : {"amenity": "school"},
                "clubs"               : {"amenity": "nightclub"},
                "pubs"                : {"amenity": "pub"},
                "parks"               : {"leisure": "park"},

                "cafes"               : {"amenity": "cafe"},
                "theatres"            : {"amenity": "theatre"},
                "cinemas"             : {"amenity": "cinema"},
                "bookshops"           : {"shop": "books"},
                "fashion_shops"       : {"shop": "clothes"},
                "unisex_toilets"      : {"toilets:unisex": "yes"}
        }

        to_amend = {}

        try:
            if radius_around_point:
                lat, lng = row[lat_column_name], row[lng_column_name]
                pois = ox.features_from_point((lat, lng), tags, dist=distance_km * 1000)
            else:
                polygon = row[geometry_column_name]
                pois = ox.features_from_polygon(polygon, tags)

            for column_name, tags in columns.items():
                filtered = pois
                for tag_key, tag_value in tags.items():
                    if tag_key in filtered.columns:
                        filtered = filtered[filtered[tag_key] == tag_value]
                    else:
                        filtered = pd.DataFrame()

                to_amend[column_name] = len(filtered)

        except:
            # InsufficientResponseError
            for column_name in columns:
                to_amend[column_name] = 0

        return pd.Series(to_amend)

    pois_counts = df.apply(count_pois, axis=1)

    df = pd.concat([df, pois_counts], axis=1)

    return df


def init_get_indicators(connection, old_table, new_table):
    progress_table_name = f"{new_table}_progress"
    index_table_name = f"{new_table}_indexes"

    indexes = run_query(connection, f"SELECT id FROM {old_table}")
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
        run_query(connection, f"DROP TABLE IF EXISTS {new_table}")


distance_km = 2
batch_size = 20


def resume_get_indicators(connection, old_table, new_table, distance_km):
    progress_table_name = f"{new_table}_progress"
    index_table_name = f"{new_table}_indexes"

    while True:
        last_processed_index = run_query(connection, f"SELECT * FROM {progress_table_name}")[
            "last_processed_index"]

        if len(last_processed_index) == 1:
            last_processed_index = last_processed_index[0]
        else:
            last_processed_index = 0

        batch_query = f"""
                    SELECT random_order, t.*
                    FROM {new_table}_indexes o
                    JOIN {old_table} t ON o.id = t.id
                    WHERE o.random_order > {last_processed_index}
                    ORDER BY o.random_order
                    LIMIT {batch_size}
                """
        batch = run_query(connection, batch_query)

        if len(batch) == 0:
            print(f"done with last processed index = {last_processed_index}")
            break

        indicators = amend_df_with_indicators(batch, radius_around_point=True,
                                              distance_km=distance_km
                                              )
        indicators.rename(columns={"id": "old_id"}, inplace=True)

        append_to_database(connection, new_table, indicators)

        new_progress = batch['random_order'].max()
        run_query(connection,
                  f"UPDATE {new_table}_progress SET last_processed_index = {new_progress}"
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
