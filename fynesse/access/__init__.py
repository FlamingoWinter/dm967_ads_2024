from . import clean
from . import fetch
from . import join
from . import metadata
from . import optimise
from . import upload


def access_part_1(connection):
    if metadata.check_previous_pipeline(connection, "part_1", 7):
        # --------------------------------------------------------------------------------------------------------------------
        # 3) Download data
        # --------------------------------------------------------------------------------------------------------------------
        raw_nssec_oa = fetch.fetch_2021_census_data(code='TS062', level='oa')
        raw_sexual_orientation_msoa = fetch.fetch_2021_census_data(code='TS077', level='msoa')

        raw_geography_oa = fetch.fetch_2021_census_geography(level='oa')
        raw_geography_msoa = fetch.fetch_2021_census_geography(level='msoa')

        # --------------------------------------------------------------------------------------------------------------------
        # 4) Connecting to the OSM API
        # --------------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------------
        # 5) Clean Data
        # --------------------------------------------------------------------------------------------------------------------
        nssec_oa = clean.clean_nssec(raw_nssec_oa)
        sexual_orientation_msoa = clean.clean_sexual_orientation(raw_sexual_orientation_msoa)
        geography_oa = clean.clean_geography_oa(raw_geography_oa)
        geography_msoa = clean.clean_geography_msoa(raw_geography_msoa)

        # --------------------------------------------------------------------------------------------------------------------
        # 6) Upload Data to the Database
        # --------------------------------------------------------------------------------------------------------------------
        df_by_table_name = {
                "sexual_orientation_msoa": sexual_orientation_msoa,
                "nssec_oa"               : nssec_oa,
                "geography_oa"           : geography_oa,
                "geography_msoa"         : geography_msoa
        }

        for table_name, df in df_by_table_name.items():
            upload.upload_to_database(connection, table_name, df)

        # --------------------------------------------------------------------------------------------------------------------
        # 7) Optimise Databases
        # --------------------------------------------------------------------------------------------------------------------
        for table_name in df_by_table_name:
            optimise.add_index(connection, table_name, "Geography")
            optimise.add_index(connection, table_name, "Geography_Code")

        for geography_table_name in ["geography_oa", "geography_msoa"]:
            optimise.add_multiple_indexes(connection, geography_table_name, ["lat", "lng"])

        # --------------------------------------------------------------------------------------------------------------------
        # 8) Join Data
        # --------------------------------------------------------------------------------------------------------------------
        join.join_tables(connection, "geography_oa", "nssec_oa", on="Geography",
                         joined_table_name="nssec_oa_geog"
                         )
        join.join_tables(connection, "geography_msoa", "sexual_orientation_msoa", on="Geography",
                         joined_table_name="sexual_orientation_msoa_geog"
                         )

        for table_name in ["nssec_oa_geog", "sexual_orientation_msoa_geog"]:
            optimise.add_index(connection, table_name, "Geography")
            optimise.add_index(connection, table_name, "Geography_Code")
            optimise.add_multiple_indexes(connection, table_name, ["lat", "lng"])

        # --------------------------------------------------------------------------------------------------------------------
        # 9) Update Metadata
        # --------------------------------------------------------------------------------------------------------------------
        metadata.update_metadata(connection, "part_1")
