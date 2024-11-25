import cx_Oracle
import csv

import os
print("Current working directory:", os.getcwd())

def get_table_count():
    try:
        # Oracle connection details
        username = "sys"  # Admin user
        password = "*********"
        hostname = "localhost"
        port = "1521"
        service_name = "ORCL"  # Replace with your Oracle service name

        # Create a DSN (Data Source Name)
        dsn = cx_Oracle.makedsn(hostname, port, service_name=service_name)

        # Establish a connection as SYSDBA
        connection = cx_Oracle.connect(user=username, password=password, dsn=dsn, mode=cx_Oracle.SYSDBA)
        cursor = connection.cursor()

        # Table name for which to count rows
        table_name = "EMPLOYEE"  # Update schema and table as needed
        sql_query = f'SELECT COUNT(*) FROM SYS."{table_name}"'

        # Execute query
        cursor.execute(sql_query)
        result = cursor.fetchone()
        row_count = result[0]

        print(f"Total number of rows in the {table_name} table: {row_count}")

        # Write result to CSV file
        output_file = "souece_table_counts.csv"
        with open(output_file, mode="w", newline="") as file:
            writer = csv.writer(file)
            # Write headers
            writer.writerow(["table_name", "source_count"])
            # Write data
            writer.writerow([table_name, row_count])

        print(f"Row count written to {output_file}")

    except cx_Oracle.DatabaseError as e:
        print("There was a problem connecting to the database or executing the query:", e)

    finally:
        # Ensure resources are cleaned up
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection is not None:
            connection.close()

if __name__ == "__main__":
    get_table_count()