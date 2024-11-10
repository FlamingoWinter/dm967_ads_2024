from datetime import datetime, timedelta

import pandas as pd
import pymysql
import requests

# This file accesses the data

"""Place commands in this file to access the data electronically. Don't remove any missing values, or deal with outliers. Make sure you have legalities correct, both intellectual property and personal data privacy rights. Beyond the legal side also think about the ethical issues around this data. """


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


def create_connection(user, password, host, database, port=3306):
    """ Create a database connection to the MariaDB database
        specified by the host url and database name.
    :param user: username
    :param password: password
    :param host: host url
    :param database: database name
    :param port: port number
    :return: Connection object or None
    """
    conn = None
    try:
        conn = pymysql.connect(user=user,
                               passwd=password,
                               host=host,
                               port=port,
                               local_infile=1,
                               db=database
                               )
        print(f"Connection established!")
    except Exception as e:
        print(f"Error connecting to the MariaDB Server: {e}")
    return conn


def run_query(conn, query):
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        return pd.DataFrame(rows, columns=columns)


def run_query(conn, query):
    with conn.cursor() as cur:
        cur.execute(query)
        if query.strip().lower().startswith(("select", "show")):
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            return pd.DataFrame(rows, columns=columns)

        elif query.strip().lower().startswith(
                ("delete", "insert", "update", "drop", "create", "alter", "kill")
        ):
            conn.commit()
            return cur.rowcount

        else:
            return None


def kill_all_processes(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("SHOW PROCESSLIST")
            processes = cursor.fetchall()
            for process in processes:
                process_id = process[0]
                user = process[1]
                command = process[4]
                if command != 'Sleep':
                    cursor.execute(f"KILL {process_id}")
                    print(f"Killed process {process_id} (user: {user}, command: {command})")
        conn.commit()

    except Exception as e:
        print(f"Error occurred: {e}")


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


def print_tables_summary(conn):
    tables = run_query(conn, "SHOW Tables");
    table_names = tables.iloc[:, 0]

    for table_name in table_names:
        print(f"\nTable: {table_name}")

        table_status = run_query(conn, f"SHOW TABLE STATUS LIKE '{table_name}';")
        if not table_status.empty:
            approx_row_count = table_status.iloc[0]['Rows']
            print(f"\nApproximate Row Count: {approx_row_count / 1_000_000:.1f} M"
                  )
        else:
            print("\nUnable to fetch row count")

        first_5_rows = run_query(conn, f"SELECT * FROM `{table_name}` LIMIT 5;")
        print(first_5_rows)

        indices = run_query(conn, f"SHOW INDEX FROM `{table_name}`");
        if not indices.empty:
            print("\nIndices:")
            for _, index in indices.iterrows():
                print(
                        f" - Index: {index['Key_name']} ({index['Index_type']}), Column: {index['Column_name']}"
                )
        else:
            print("\nNo indices set on this table.")
