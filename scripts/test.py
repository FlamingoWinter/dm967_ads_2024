import os

import requests
from dotenv import load_dotenv

from fynesse.common.db.db_setup import create_connection

load_dotenv()

url = os.getenv('DB_URL')
user = os.getenv('DB_LOGIN')
password = os.getenv("DB_PASSWORD")
port = int(os.getenv('DB_PORT'))

connection = create_connection(user, password=password, host=url, port=port,
                               database="ads_2024_final"
                               )

from fynesse.access import metadata
from fynesse.assess import query


def download_price_paid_data(year, part):
    file_path = f"./pp-{year}-part{part}.csv"

    if os.path.exists(file_path):
        print(f"file  already exists. skipping")
        return

    base_url = f"http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-{str(year)}-part{str(part)}.csv"
    with requests.get(base_url, stream=True) as response:
        response.raise_for_status()
        with open(file_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)


def pipeline_to_upload_houses(connection, houses_table_name):
    progress_table_name = f"upload_houses_progress"

    if metadata.check_previous_pipeline(connection, "upload_houses", 1000):
        if not query.abort_deletion_if_table_exists(connection, progress_table_name):
            create_query = f"""
                            CREATE TABLE {progress_table_name} (
                            id INTEGER PRIMARY KEY AUTO_INCREMENT, last_processed_year INTEGER NOT NULL, last_processed_part INTEGER NOT NULL
                        );
                        """
            query.run_query(connection, create_query)
            query.run_query(connection,
                            f"INSERT INTO {progress_table_name} (last_processed_year, last_processed_part) VALUES (1994, 2)"
                            )
            query.run_query(connection, f"DROP TABLE IF EXISTS {houses_table_name}")
            query.run_query(connection, f"""CREATE TABLE IF NOT EXISTS `{houses_table_name}` (
                      `transaction_unique_identifier` tinytext COLLATE utf8_bin NOT NULL,
                      `price` int(10) unsigned NOT NULL,
                      `date_of_transfer` date NOT NULL,
                      `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
                      `property_type` varchar(1) COLLATE utf8_bin NOT NULL,
                      `new_build_flag` varchar(1) COLLATE utf8_bin NOT NULL,
                      `tenure_type` varchar(1) COLLATE utf8_bin NOT NULL,
                      `primary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
                      `secondary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
                      `street` tinytext COLLATE utf8_bin NOT NULL,
                      `locality` tinytext COLLATE utf8_bin NOT NULL,
                      `town_city` tinytext COLLATE utf8_bin NOT NULL,
                      `district` tinytext COLLATE utf8_bin NOT NULL,
                      `county` tinytext COLLATE utf8_bin NOT NULL,
                      `ppd_category_type` varchar(2) COLLATE utf8_bin NOT NULL,
                      `record_status` varchar(2) COLLATE utf8_bin NOT NULL,
                      `db_id` bigint(20) unsigned NOT NULL
                    ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1 ;
                    """
                            )

        metadata.update_metadata(connection, "upload_houses")

    while True:
        last_processed = query.run_query(connection, f"SELECT * FROM {progress_table_name}")

        last_processed_year = last_processed.iloc[[0]]["last_processed_year"][0]
        last_processed_part = last_processed.iloc[[0]]["last_processed_part"][0]

        if last_processed_part == 2:
            part_to_process = 1
            year_to_process = last_processed_year + 1
        else:
            part_to_process = last_processed_part + 1
            year_to_process = last_processed_year

        if year_to_process > 2024:
            print("Finished processing up to 2024")
            break

        download_price_paid_data(year_to_process, part_to_process)

        query.run_query(connection, f"""
                LOAD DATA LOCAL INFILE "./pp-{year_to_process}-part{part_to_process}.csv" 
                INTO TABLE `{houses_table_name}` FIELDS TERMINATED BY ',' 
                OPTIONALLY ENCLOSED by '"' LINES STARTING BY '' 
                TERMINATED BY '\n';
        """
                        )

        query.run_query(connection,
                        f"UPDATE {progress_table_name} SET last_processed_year = {year_to_process}, last_processed_part = {part_to_process}"
                        )

        print(f"Processed part {part_to_process} of year {year_to_process}")


pipeline_to_upload_houses(connection, "price_paid")
