from typing import Union

import pandas as pd
from pymysql import Connection


def run_query(connection: Connection, query: str, args=None, execute_many: bool = False
              ) -> Union[pd.DataFrame, None, int]:
    with connection.cursor() as cursor:
        cursor.execute(query, args) if not execute_many else cursor.executemany(query, args)

        if query.strip().lower().startswith(("select", "show", "describe")):
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return pd.DataFrame(rows, columns=columns)

        elif query.strip().lower().startswith(
                ("delete", "insert", "update", "drop", "create", "alter", "kill")
        ):
            connection.commit()
            return cursor.rowcount

        else:
            return None


def kill_all_processes(connection: Connection):
    try:
        run_query(connection, "SHOW PROCESSLIST")

        with connection.cursor() as cursor:
            cursor.execute("SHOW PROCESSLIST")
            processes = cursor.fetchall()
            for process in processes:
                process_id = process[0]
                user = process[1]
                command = process[4]
                if command != 'Sleep':
                    cursor.execute(f"KILL {process_id}")
                    print(f"Killed process {process_id} (user: {user}, command: {command})")
        connection.commit()

    except Exception as e:
        print(f"Error occurred: {e}")


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


def abort_deletion_if_table_exists(connection, table_name):
    tables = run_query(connection, "SHOW TABLES")
    if table_name in tables[tables.columns[-1]].to_list():
        delete_confirmation = input(
                f"Table '{table_name}' already exists. Do you want to dellete the table and recreate it? (y/n): "
        ).strip().lower()
        if delete_confirmation in ["y", "yes"]:
            print(f"Deleting table '{table_name}'...")
            run_query(connection, f"DROP TABLE {table_name}")
            print(f"Deleted")
        else:
            print("Aborting upload.")
            return True
    return False


def column_exists(connection, table_name, column_name):
    cursor = connection.cursor()

    check_query = f"""
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
    AND table_name = '{table_name}'
    AND column_name = '{column_name}';
    """

    cursor.execute(check_query)
    return cursor.fetchone()[0] != 0


def add_key(conn, table_name):
    if not column_exists(conn, table_name, "id"):
        run_query(conn, f"ALTER TABLE {table_name} ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY;")
