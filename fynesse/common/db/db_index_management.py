from ..db.db import run_query


def index_exists(connection, table_name, index_name):
    cursor = connection.cursor()

    check_query = f"""
    SELECT COUNT(*)
    FROM information_schema.statistics
    WHERE table_schema = DATABASE()
    AND table_name = '{table_name}'
    AND index_name = '{index_name}';
    """

    cursor.execute(check_query)
    return cursor.fetchone()[0] != 0


def add_index(connection, table_name, column_name):
    index_name = f"{column_name}_idx"

    if index_exists(connection, table_name, index_name):
        print(f"index {index_name} already exists on {table_name}")
        return

    run_query(connection, f"CREATE INDEX {index_name} ON {table_name} ({column_name})")


def add_multiple_indexes(connection, table_name, column_names):
    index_name = f"{'_'.join(column_names)}_idx"

    if index_exists(connection, table_name, index_name):
        print(f"index {index_name} already exists on {table_name}")
        return

    run_query(connection, f"CREATE INDEX {index_name} ON {table_name} ({', '.join(column_names)})")
