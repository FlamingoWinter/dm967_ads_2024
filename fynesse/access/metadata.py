from pymysql import Connection

from fynesse.common.db import db_setup


def create_metadata_table(conn: Connection):
    return db_setup.create_metadata_table(conn)


def check_previous_upload(connection, pipeline_name):
    db_setup.check_previous_upload(connection, pipeline_name)


def update_metadata(connection, pipeline_name):
    db_setup.update_metadata(connection, "practical 1")
