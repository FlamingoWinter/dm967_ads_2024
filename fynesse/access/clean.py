def clean_nssec(raw_nssec):
    nssec = raw_nssec.drop(raw_nssec.columns[[0, 3]], axis=1)
    nssec.columns = ["Geography", "Geography_Code", "L1-L3", "L4-L6", "L7", "L8-9", "L10-L11",
                     "L12", "L13", "L14", "L15"]
    return nssec


def clean_sexual_orientation(raw_sexual_orientation):
    sexual_orientation = raw_sexual_orientation.drop(raw_sexual_orientation.columns[[0, 3]], axis=1)
    sexual_orientation.columns = ["Geography", "Geography_Code", "Heterosexual", "Gay_or_Lesbian",
                                  "Bisexual", "Other", "Not_Answered"]
    return sexual_orientation


def clean_geography_oa(raw_geography_oa):
    geography_oa = raw_geography_oa.drop(raw_geography_oa.columns[[0, 2, 4, 5, 6, 9]], axis=1)
    geography_oa.columns = ["Geography_Code", "Geography", "lat", "lng", "geometry"]
    return geography_oa


def clean_geography_msoa(raw_geography_msoa):
    geography_msoa = raw_geography_msoa.drop(raw_geography_msoa.columns[[0, 3, 4, 5, 8]], axis=1)
    geography_msoa.columns = ["Geography_Code", "Geography", "lat", "lng", "geometry"]
    return geography_msoa


def calculate_nssec_totals(nssec):
    columns_to_sum = ["L1-L3", "L4-L6", "L7", "L8-9", "L10-L11", "L12", "L13", "L14", "L15"]

    nssec["total"] = sum(nssec.get(col, 0) for col in columns_to_sum)
    return nssec


def calculate_so_totals(so):
    columns_to_sum = ["Heterosexual", "Gay_or_Lesbian", "Bisexual", "Other", "Not_Answered"]

    so["total"] = sum(so.get(col, 0) for col in columns_to_sum)
    return so


def clean_work_type(raw_work_type):
    work_type = raw_work_type.pivot_table(
            index=["Output Areas Code", "Output Areas"],
            columns="Occupation (current) (10 categories)",
            values="Observation",
            aggfunc="first"
    ).reset_index()
    work_type.columns = ["Geography_Code", "Geography", "managers_directors_senior_officials",
                         "professional",
                         "associate_professional_technical",
                         "administrative_secretarial",
                         "skilled_trades",
                         "caring_leisure_other_service",
                         "sales_customer_service",
                         "process_plant_machine_operatives",
                         "elementary",
                         "does_not_apply"]
    work_type = work_type.drop(columns=["Geography"])
    return work_type


def clean_commuter_distance(raw_commuter_distance):
    commuter_distance = raw_commuter_distance.drop(raw_commuter_distance.columns[[0]], axis=1)
    commuter_distance.columns = [
            "Geography",
            "Geography_Code",
            "dist_total_all_usual_residents",
            "dist_less_than_2km",
            "dist_2km_to_less_than_5km",
            "dist_5km_to_less_than_10km",
            "dist_10km_to_less_than_20km",
            "dist_20km_to_les_than_30km",
            "dist_30km_to_less_than_40km",
            "dist_40km_to_less_than_60km",
            "dist_60km_and_over",
            "dist_works_mainly_from_home",
            "dist_offshore_no_fixed_place_outside_uk"
    ]
    commuter_distance = commuter_distance.drop(columns=["Geography"])
    return commuter_distance


def clean_hours_worked(raw_hours_worked):
    hours_worked = raw_hours_worked.drop(raw_hours_worked.columns[[0]], axis=1)
    hours_worked.columns = [
            "geography",
            "geography_code",
            "hours_total_all_usual_residents",
            "hours_part_time",
            "hours_part_time_15_or_less",
            "hours_part_time_16_to_30",
            "hours_fulltime",
            "hours_fulltime_31_to_48",
            "hours_fulltime_49_or_more"
    ]
    hours_worked = hours_worked.drop(columns=["Geography"])
    return hours_worked
