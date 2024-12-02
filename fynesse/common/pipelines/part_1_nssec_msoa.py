from fynesse.access import *


def init_part_1_nssec_msoa(connection):
    raw_nssec_msoa = fetch.fetch_2021_census_data(code='TS062', level='msoa')

    nssec_msoa = clean.clean_nssec(raw_nssec_msoa)

    upload.upload_to_database(connection, "nssec_msoa", nssec_msoa)

    optimise.add_index(connection, "nssec_msoa", "Geography")
    optimise.add_index(connection, "nssec_msoa", "Geography_Code")

    join.join_tables(connection, "sexual_orientation_msoa_geog", "nssec_msoa", on="Geography",
                     joined_table_name="sexual_orientation_nssec_msoa_geog"
                     )

    optimise.add_index(connection, "sexual_orientation_nssec_msoa_geog", "Geography")
    optimise.add_index(connection, "sexual_orientation_nssec_msoa_geog", "Geography_Code")
    optimise.add_multiple_indexes(connection, "sexual_orientation_nssec_msoa_geog", ["lat", "lng"])

    optimise.add_key(connection, "sexual_orientation_nssec_msoa_geog")

    print("Pipeline completed...")
