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
