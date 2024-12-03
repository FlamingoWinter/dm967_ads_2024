import pandas as pd

from fynesse.common.db.db_operations import upload_to_database


def init_upload_work_type_relationships(connection):
    url = "https://github.com/FlamingoWinter/ads_practicals/raw/refs/heads/main/hours_worked_by_detailed_work_type.csv"
    raw_hours_worked_by_detailed_work_type = pd.read_csv(url)

    hours_worked_by_detailed_work_type = raw_hours_worked_by_detailed_work_type.pivot_table(
            index=["Occupation (current) (105 categories) Code",
                   "Occupation (current) (105 categories)"],
            columns="Hours worked (5 categories)",
            values="Observation",
            aggfunc="first"
    ).reset_index()

    hours_worked_by_detailed_work_type = hours_worked_by_detailed_work_type.reset_index(drop=True)
    hours_worked_by_detailed_work_type.columns = [
            'occupation_code',
            'occupation',
            'hours_na',
            'hours_31_to_48',
            'hours_49_plus',
            'hours_0_to_15',
            'hours_16_to_30'
    ]

    url = "https://github.com/FlamingoWinter/ads_practicals/raw/refs/heads/main/hours_worked_by_work_type.csv"
    raw_hours_worked_by_work_type = pd.read_csv(url)

    hours_worked_by_work_type = raw_hours_worked_by_work_type.pivot_table(
            index=["Occupation (current) (10 categories) Code",
                   "Occupation (current) (10 categories)"],
            columns="Hours worked (5 categories)",
            values="Observation",
            aggfunc="first"
    ).reset_index()

    hours_worked_by_work_type = hours_worked_by_work_type.reset_index(drop=True)
    hours_worked_by_work_type.columns = [
            'occupation_code',
            'occupation',
            'hours_na',
            'hours_31_to_48',
            'hours_49_plus',
            'hours_0_to_15',
            'hours_16_to_30'
    ]

    url = "https://github.com/FlamingoWinter/ads_practicals/raw/refs/heads/main/distance_travelled_by_work_type.csv"
    raw_distance_travelled_by_work_type = pd.read_csv(url)

    distance_travelled_by_work_type = raw_distance_travelled_by_work_type.pivot_table(
            index=["Occupation (current) (10 categories) Code",
                   "Occupation (current) (10 categories)"],
            columns="Distance travelled to work (8 categories)",
            values="Observation",
            aggfunc="first"
    ).reset_index()

    distance_travelled_by_work_type = distance_travelled_by_work_type.reset_index(drop=True)
    distance_travelled_by_work_type.columns = [
            'occupation_code',
            'occupation',
            'distance_10_to_30km',
            'distance_30_to_60km',
            'distance_5_to_10km',
            'distance_60km_plus',
            'not_applicable',
            'distance_less_than_5km',
            'distance_works_offshore_or_abroad',
            'distance_works_from_home'
    ]

    url = "https://github.com/FlamingoWinter/ads_practicals/raw/refs/heads/main/distance_travelled_by_detailed_work_type.csv"
    raw_distance_travelled_by_detailed_work_type = pd.read_csv(url)

    distance_travelled_by_detailed_work_type = raw_distance_travelled_by_detailed_work_type.pivot_table(
            index=["Occupation (current) (105 categories) Code",
                   "Occupation (current) (105 categories)"],
            columns="Distance travelled to work (8 categories)",
            values="Observation",
            aggfunc="first"
    ).reset_index()

    distance_travelled_by_detailed_work_type = distance_travelled_by_detailed_work_type.reset_index(
            drop=True
    )
    distance_travelled_by_detailed_work_type.columns = [
            'occupation_code',
            'occupation',
            'distance_10_to_30km',
            'distance_30_to_60km',
            'distance_5_to_10km',
            'distance_60km_plus',
            'not_applicable',
            'distance_less_than_5km',
            'distance_works_offshore_or_abroad',
            'distance_works_from_home'
    ]

    upload_to_database(connection, "hours_worked_by_detailed_work_type",
                       hours_worked_by_detailed_work_type
                       )
    upload_to_database(connection, "hours_worked_by_work_type", hours_worked_by_work_type)
    upload_to_database(connection, "distance_travelled_by_detailed_work_type",
                       hours_worked_by_detailed_work_type
                       )
    upload_to_database(connection, "distance_travelled_by_work_type",
                       hours_worked_by_work_type
                       )
