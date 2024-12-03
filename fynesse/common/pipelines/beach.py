import geopandas as gpd
import pandas as pd
import unicodedata
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry
from shapely.wkt import loads

from fynesse.access.fetch import fetch_coast, fetch_uk_beaches
from fynesse.assess.query import database_df_to_gpd
from fynesse.common.db.db import run_query, add_key, abort_deletion_if_table_exists
from fynesse.common.db.db_operations import upload_to_database


def remove_annoying_characters(name):
    if isinstance(name, str):
        return unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('ascii')
    return name


def init_beach_pipeline(connection):
    if abort_deletion_if_table_exists(connection, "beach"):
        return
    if abort_deletion_if_table_exists(connection, "beach_intersects_oa"):
        return

    coast = fetch_coast()
    uk_beaches = fetch_uk_beaches()

    coast_buffered = coast.to_crs("EPSG:27700")
    coast_buffered['geometry'] = coast_buffered.geometry.buffer(200)
    coast_buffered.set_geometry('geometry', inplace=True)
    coast_buffered = coast_buffered.to_crs("EPSG:4326")

    coast_buffered.rename(
            columns={col: f"{col}_right" for col in coast_buffered.columns if col != "geometry"},
            inplace=True
    )

    coastal_beaches = gpd.sjoin(uk_beaches, coast_buffered, how="inner", predicate="intersects")

    coastal_beaches = coastal_beaches.drop(
            columns=[col for col in coastal_beaches.columns if col.endswith('_right')]
    )
    coastal_beaches.columns = uk_beaches.columns

    unique_beaches = coastal_beaches.drop_duplicates(subset="id")

    oa_boundaries = run_query(connection, "SELECT id as oa_id, geometry FROM oa")
    oa_buffered_boundaries = database_df_to_gpd(oa_boundaries)
    oa_buffered_boundaries = oa_buffered_boundaries.to_crs("EPSG:27700")
    oa_buffered_boundaries['geometry'] = oa_buffered_boundaries.geometry.buffer(50)
    oa_buffered_boundaries = oa_buffered_boundaries.to_crs("EPSG:4326")

    oa_buffered_boundaries.rename(
            columns={col: f"{col}_right" for col in oa_buffered_boundaries.columns if
                     col != "geometry"},
            inplace=True
    )

    beaches_with_oa = gpd.sjoin(unique_beaches, oa_buffered_boundaries, how="inner",
                                predicate="intersects"
                                )

    duplicates = beaches_with_oa.duplicated(subset=['lat', 'lng'], keep='first')

    beaches_with_oa_deduplicated = beaches_with_oa[~duplicates]

    beaches_with_oa_relevant = beaches_with_oa_deduplicated[
        ["name", "surface", "lat", "lng", "geometry"]]

    beaches_with_oa_relevant = beaches_with_oa_relevant.where(pd.notnull(beaches_with_oa_relevant),
                                                              None
                                                              )

    beaches_with_oa_relevant["geometry"] = beaches_with_oa_relevant["geometry"].apply(
            lambda g: g.simplify(0.001) if isinstance(g, BaseGeometry) else
            (shape(loads(g)).simplify(0.001) if isinstance(g, str) else None)
    )

    beaches_with_oa_relevant['name'] = beaches_with_oa_relevant['name'].apply(
            remove_annoying_characters
    )
    beaches_with_oa_relevant = beaches_with_oa_relevant.dropna(subset=['lat'])
    beaches_with_oa_relevant = beaches_with_oa_relevant.dropna(subset=['lng'])

    upload_to_database(connection, "beach", beaches_with_oa_relevant)
    add_key(connection, "beach")

    beach_to_oa = beaches_with_oa[["lat", "lng", "oa_id"]]

    beach_to_lat_lng = run_query(connection, "SELECT * FROM beach")

    beach_to_lat_lng = beach_to_lat_lng.merge(beach_to_oa, on=["lat", "lng"], how="inner")[
        ["id", "oa_id"]]

    beach_to_lat_lng = beach_to_lat_lng.rename(columns={"id": "beach_id"})

    upload_to_database(connection, "beach_intersects_oa", beach_to_lat_lng)
