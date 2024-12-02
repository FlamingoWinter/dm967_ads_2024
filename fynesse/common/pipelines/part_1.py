from fynesse.access import *


def init_part_1(connection):
    print("Fetching Data...")

    raw_nssec_oa = fetch.fetch_2021_census_data(code='TS062', level='oa')
    raw_sexual_orientation_msoa = fetch.fetch_2021_census_data(code='TS077', level='msoa')

    raw_geography_oa = fetch.fetch_2021_census_geography(level='oa')
    raw_geography_msoa = fetch.fetch_2021_census_geography(level='msoa')

    print("Cleaning Data...")

    nssec_oa = clean.clean_nssec(raw_nssec_oa)
    sexual_orientation_msoa = clean.clean_sexual_orientation(raw_sexual_orientation_msoa)
    geography_oa = clean.clean_geography_oa(raw_geography_oa)
    geography_msoa = clean.clean_geography_msoa(raw_geography_msoa)

    df_by_table_name = {
            "sexual_orientation_msoa": sexual_orientation_msoa,
            "nssec_oa"               : nssec_oa,
            "geography_oa"           : geography_oa,
            "geography_msoa"         : geography_msoa
    }

    print("Uploading Data to SQL Database...")

    for table_name, df in df_by_table_name.items():
        upload.upload_to_database(connection, table_name, df)

    print("Adding indexes to tables...")

    for table_name in df_by_table_name:
        optimise.add_index(connection, table_name, "Geography")
        optimise.add_index(connection, table_name, "Geography_Code")

    for geography_table_name in ["geography_oa", "geography_msoa"]:
        optimise.add_multiple_indexes(connection, geography_table_name, ["lat", "lng"])

    print("Creating in-database joined tables...")

    join.join_tables(connection, "geography_oa", "nssec_oa", on="Geography_Code",
                     joined_table_name="nssec_oa_geog"
                     )
    join.join_tables(connection, "geography_msoa", "sexual_orientation_msoa", on="Geography",
                     joined_table_name="sexual_orientation_msoa_geog"
                     )

    print("Adding indexes to in-database joined tables...")

    for table_name in ["nssec_oa_geog", "sexual_orientation_msoa_geog"]:
        optimise.add_index(connection, table_name, "Geography")
        optimise.add_index(connection, table_name, "Geography_Code")
        optimise.add_multiple_indexes(connection, table_name, ["lat", "lng"])

    optimise.add_key(connection, "sexual_orientation_msoa_geog")
    optimise.add_key(connection, "nssec_oa_geog")

    print("Pipeline Completed")
