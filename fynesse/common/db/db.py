import pandas as pd


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
    cursor = connection.cursor()
    cursor.execute("SHOW TABLES")
    tables = [row[0] for row in cursor.fetchall()]
    if table_name in tables:
        delete_confirmation = input(
                f"Table '{table_name}' already exists. Do you want to dellete the table and recreate it? (y/n): "
        ).strip().lower()
        if delete_confirmation in ["y", "yes"]:
            clear_table_query = f"DROP TABLE {table_name}"
            print(f"Deleting table '{table_name}'...")
            cursor.execute(clear_table_query)
        else:
            print("Aborting upload.")
            return True
    return False
