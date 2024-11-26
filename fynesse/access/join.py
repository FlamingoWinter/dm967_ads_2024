from ..common.db import db_operations


def join_tables(connection, table1, table2, on, joined_table_name):
    return db_operations.join_tables(connection, table1, table2, on, joined_table_name)
