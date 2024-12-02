from fynesse.access.clean import *
from fynesse.access.fetch import *
from fynesse.common.db.db_operations import join_in_place


def init_add_census_data(connection):
    raw_hours_worked_by_oa = fetch_2021_census_data('TS059', 'oa')
    hours_worked_by_oa = clean_hours_worked(raw_hours_worked_by_oa)

    raw_commuter_distance_by_oa = fetch_2021_census_data('TS058', 'oa')
    commuter_distance_by_oa = clean_commuter_distance(raw_commuter_distance_by_oa)

    url = "https://github.com/FlamingoWinter/ads_practicals/raw/refs/heads/main/occupation_by_oa.zip"
    raw_work_type_by_oa = fetch_2021_census_data('TS063', 'oa',
                                                 custom_url=url
                                                 )
    work_type_by_oa = clean_work_type(raw_work_type_by_oa)

    join_in_place(connection, "oa", hours_worked_by_oa, on=["Geography_Code"])
    join_in_place(connection, "oa", commuter_distance_by_oa, on=["Geography_Code"])
    join_in_place(connection, "oa", work_type_by_oa, on=["Geography_Code"])
