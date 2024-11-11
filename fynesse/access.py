from datetime import datetime, timedelta

import requests

from fynesse import sql


def run_query(conn, query):
    sql.run_query(conn, query)


def kill_all_processes(conn):
    sql.kill_all_processes(conn)


def print_tables_summary(conn):
    sql.print_tables_summary(conn)


def create_connection(user, password, host, database, port=3306):
    sql.create_connection(user, password, host, database, port)


def data():
    """Read the data from the web or local file, returning structured format such as a data frame"""
    download_price_paid_data(2011, 2020)


def download_price_paid_data(year_from, year_to):
    base_url = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com"
    file_name = "/pp-<year>-part<part>.csv"
    for year in range(year_from, (year_to + 1)):
        print(f"Downloading data for year: {year}")
        for part in range(1, 3):
            url = base_url + file_name.replace("<year>", str(year)).replace("<part>", str(part))
            response = requests.get(url)
            if response.status_code == 200:
                with open("." + file_name.replace("<year>", str(year)).replace("<part>", str(part)),
                          "wb"
                          ) as file:
                    file.write(response.content)


def housing_upload_join_data(conn, year):
    print(f'Selecting data for year: {year}')

    start_date = datetime(year, 1, 1)
    end_date = datetime(year, 12, 31)

    current_date = start_date
    while current_date <= end_date:
        with conn.cursor() as cur:
            day_str = current_date.strftime('%Y-%m-%d')
            cur.execute(
                    f"""
                SELECT 1
                FROM prices_coordinates_data
                WHERE `date_of_transfer`  = '{day_str}'
                LIMIT 1
                """
            )
            result = cur.fetchone()
            if result:
                print(f"Data for {day_str} is already inserted")
            else:
                cur.execute(
                        f"""
                    SELECT pp.price, pp.date_of_transfer, po.postcode, pp.property_type, pp.new_build_flag,
                           pp.tenure_type, pp.locality, pp.town_city, pp.district, pp.county, po.country, 
                           po.latitude, po.longitude, pp.primary_addressable_object_name
                    FROM (
                        SELECT price, date_of_transfer, postcode, property_type, new_build_flag, tenure_type, 
                               locality, town_city, district, county, primary_addressable_object_name
                        FROM pp_data 
                        WHERE date_of_transfer BETWEEN '{day_str} 00:00:00' AND '{day_str} 23:59:59'
                    ) AS pp
                    INNER JOIN postcode_data AS po ON pp.postcode = po.postcode
                    """
                )
                rows = cur.fetchall()

                if rows:
                    insert_query = """
                        INSERT INTO prices_coordinates_data (
                            price, date_of_transfer, postcode, property_type, new_build_flag,
                            tenure_type, locality, town_city, district, county, country, 
                            latitude, longitude, primary_addressable_object_name
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cur.executemany(insert_query, rows)
                    conn.commit()
                    print(f'Data stored for day: {day_str}')
                else:
                    print(f"No data found for {day_str}")

        # Move to the next day
        current_date += timedelta(days=1)

    print(f"Data upload complete for year: {year}")
