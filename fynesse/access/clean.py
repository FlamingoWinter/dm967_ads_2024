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


def calculate_so_totals(so):
    columns_to_sum = ["Heterosexual", "Gay_or_Lesbian", "Bisexual", "Other", "Not_Answered"]

    so["total"] = sum(so.get(col, 0) for col in columns_to_sum)
