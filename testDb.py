import cx_Oracle

def get_table_count():
    try:
        # Oracle connection details
        username = "sys"  # Admin user
        password = "*****"
        hostname = "localhost"
        port = "1521"
        service_name = "ORCL"  # Replace with your Oracle service name

        # Create a DSN (Data Source Name)
        dsn = cx_Oracle.makedsn(hostname, port, service_name=service_name)

        # Establish a connection as SYSDBA
        connection = cx_Oracle.connect(user=username, password=password, dsn=dsn, mode=cx_Oracle.SYSDBA)
        cursor = connection.cursor()

        # Correct SQL query (remove semicolon, handle possible reserved word issues)
        sql_query = 'SELECT COUNT(*) FROM SYS."EMPLOYEE"'  # Update schema and table as needed

        # Execute query
        cursor.execute(sql_query)
        result = cursor.fetchone()

        print(f"Total number of rows in the EMPLOYEE table: {result[0]}")

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