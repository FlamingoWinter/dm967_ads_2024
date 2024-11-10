import os

from dotenv import load_dotenv

from fynesse import access

load_dotenv()

DB_LOGIN = os.getenv('DB_LOGIN')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_PORT = os.getenv('DB_PORT')
DB_URL = os.getenv('DB_URL')


def connect():
    return access.create_connection(
            DB_LOGIN, DB_PASSWORD, DB_URL, "ads_2024", int(DB_PORT)
    )


def up():
    connection = connect()
    access.run_query(connection, """
                DROP table if exists prices_coordinates_data
            """
                     );

    access.run_query(connection, """
                CREATE TABLE prices_coordinates_data (
                price REAL,
                date_of_transfer TEXT,
                postcode TEXT,
                property_type TEXT,
                new_build_flag TEXT,
                tenure_type TEXT,
                locality TEXT,
                town_city TEXT,
                district TEXT,
                county TEXT,
                country TEXT,
                latitude REAL,
                longitude REAL,
                primary_addressable_object_name TEXT);
            """
                     );
    access.run_query(connection,
                     "CREATE INDEX idx_date_of_transfer ON prices_coordinates_data(date_of_transfer);"
                     )
