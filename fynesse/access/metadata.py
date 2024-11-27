from pymysql import Connection

from fynesse.common.db import db_setup


def create_metadata_table(conn: Connection):
    return db_setup.create_metadata_table(conn)


def check_previous_pipeline(connection, pipeline_name, expected_minutes):
    return db_setup.check_previous_pipeline(connection, pipeline_name, expected_minutes)


def update_metadata(connection, pipeline_name):
    return db_setup.update_metadata(connection, pipeline_name)
