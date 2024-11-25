import cx_Oracle

def create_and_populate_depart_table():
    try:
        # Oracle connection details
        username = "sys"  # Admin user
        password = "*******8"
        hostname = "localhost"
        port = "1521"
        service_name = "ORCL"

        # Create a DSN (Data Source Name)
        dsn = cx_Oracle.makedsn(hostname, port, service_name=service_name)

        # Establish a connection as SYSDBA
        connection = cx_Oracle.connect(user=username, password=password, dsn=dsn, mode=cx_Oracle.SYSDBA)
        cursor = connection.cursor()

        # SQL commands to create and populate the table
        create_table_query = """
        CREATE TABLE SYS.DEPART (
            DEPT_ID NUMBER PRIMARY KEY,
            DEPT_NAME VARCHAR2(100),
            LOCATION VARCHAR2(100)
        )
        """
        insert_data_queries = [
            "INSERT INTO SYS.DEPART (DEPT_ID, DEPT_NAME, LOCATION) VALUES (1, 'HR', 'New York')",
            "INSERT INTO SYS.DEPART (DEPT_ID, DEPT_NAME, LOCATION) VALUES (2, 'Finance', 'London')",
            "INSERT INTO SYS.DEPART (DEPT_ID, DEPT_NAME, LOCATION) VALUES (3, 'IT', 'San Francisco')",
            "INSERT INTO SYS.DEPART (DEPT_ID, DEPT_NAME, LOCATION) VALUES (4, 'Marketing', 'Chicago')"
        ]

        # Execute the create table query
        cursor.execute(create_table_query)
        print("DEPART table created successfully.")

        # Insert sample data
        for query in insert_data_queries:
            cursor.execute(query)

        # Commit the changes
        connection.commit()
        print("Sample data inserted into DEPART table.")

    except cx_Oracle.DatabaseError as e:
        print("Error occurred:", e)

    finally:
        # Ensure resources are cleaned up
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection is not None:
            connection.close()

if __name__ == "__main__":
    create_and_populate_depart_table()