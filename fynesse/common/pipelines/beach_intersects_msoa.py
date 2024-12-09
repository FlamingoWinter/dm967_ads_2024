import geopandas as gpd

from fynesse.access.upload import upload_to_database
from fynesse.assess.query import database_df_to_gpd
from fynesse.common.db.db import run_query
from fynesse.common.db.db_index_management import add_index


def init_beach_intersects_msoa(connection):
    run_query(connection, "DROP TABLE IF EXISTS beach_intersects_msoa")

    msoa_boundaries = run_query(connection, "SELECT id as msoa_id, geometry FROM msoa")
    msoa_buffered_boundaries = database_df_to_gpd(msoa_boundaries)
    msoa_buffered_boundaries = msoa_buffered_boundaries.to_crs("EPSG:27700")
    msoa_buffered_boundaries['geometry'] = msoa_buffered_boundaries.geometry.buffer(50)
    msoa_buffered_boundaries = msoa_buffered_boundaries.to_crs("EPSG:4326")

    msoa_buffered_boundaries.rename(
            columns={col: f"{col}_right" for col in msoa_buffered_boundaries.columns if
                     col != "geometry"},
            inplace=True
    )

    unique_beaches = run_query(connection, "SELECT id as beach_id, geometry FROM beach")
    unique_beaches = database_df_to_gpd(unique_beaches, crs="EPSG:4326")

    beaches_with_msoa = gpd.sjoin(unique_beaches, msoa_buffered_boundaries, how="inner",
                                  predicate="intersects"
                                  )

    beaches_with_msoa.rename(columns={"msoa_id_right": "msoa_id"}, inplace=True)

    upload_to_database(connection, "beach_intersects_msoa",
                       beaches_with_msoa[["beach_id", "msoa_id"]]
                       )
    add_index(connection, "beach_intersects_oa", "oa_id")
    add_index(connection, "beach_intersects_oa", "beach_id")
    add_index(connection, "beach_intersects_msoa", "msoa_id")
    add_index(connection, "beach_intersects_msoa", "beach_id")
