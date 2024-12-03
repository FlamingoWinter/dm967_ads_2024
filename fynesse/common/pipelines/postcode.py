import io
import zipfile

import requests

from fynesse.access import optimise
from fynesse.common.db.db import run_query


def init_postcode(connection):
    run_query(connection, "DROP TABLE IF EXISTS postcode")
    run_query(connection, """CREATE TABLE IF NOT EXISTS `postcode` (
                `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
                `status` enum('live','terminated') NOT NULL,
                `usertype` enum('small', 'large') NOT NULL,
                `easting` int unsigned,
                `northing` int unsigned,
                `positional_quality_indicator` int NOT NULL,
                `country` enum('England', 'Wales', 'Scotland', 'Northern Ireland', 'Channel Islands', 'Isle of Man') NOT NULL,
                `latitude` decimal(11,8) NOT NULL,
                `longitude` decimal(10,8) NOT NULL,
                `postcode_no_space` tinytext COLLATE utf8_bin NOT NULL,
                `postcode_fixed_width_seven` varchar(7) COLLATE utf8_bin NOT NULL,
                `postcode_fixed_width_eight` varchar(8) COLLATE utf8_bin NOT NULL,
                `postcode_area` varchar(2) COLLATE utf8_bin NOT NULL,
                `postcode_district` varchar(4) COLLATE utf8_bin NOT NULL,
                `postcode_sector` varchar(6) COLLATE utf8_bin NOT NULL,
                `outcode` varchar(4) COLLATE utf8_bin NOT NULL,
                `incode` varchar(3)  COLLATE utf8_bin NOT NULL
              ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin;"""
              )

    url = "https://www.getthedata.com/downloads/open_postcode_geo.csv.zip"
    response = requests.get(url)
    if response.status_code == 200:
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            with z.open("open_postcode_geo.csv") as extracted_file:
                with open("postcodes.csv", "wb") as outfile:
                    outfile.write(extracted_file.read())

        run_query(connection, """
        LOAD DATA LOCAL INFILE "./postcodes.csv" INTO TABLE `postcode` FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED by '"' LINES STARTING BY '' TERMINATED BY "\n";
        """
                  )

    optimise.add_key(connection, "postcode")
