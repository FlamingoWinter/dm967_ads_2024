from fynesse.common.db import db_index_management


def add_index(connection, table_name, column_name):
    return db_index_management.add_index(connection, table_name, column_name)


def add_multiple_indexes(connection, table_name, column_names):
    return db_index_management.add_multiple_indexes(connection, table_name, column_names)
