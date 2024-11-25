import cx_Oracle
import csv


def get_table_counts_from_file():
    try:
        # Oracle connection details
        username = "sys"  # Admin user
        password = "********"
        hostname = "localhost"
        port = "1521"
        service_name = "ORCL"  # Replace with your Oracle service name

        # Create a DSN (Data Source Name)
        dsn = cx_Oracle.makedsn(hostname, port, service_name=service_name)

        # Establish a connection as SYSDBA
        connection = cx_Oracle.connect(user=username, password=password, dsn=dsn, mode=cx_Oracle.SYSDBA)
        cursor = connection.cursor()

        # Read SQL queries from the queries.txt file
        with open('queries.txt', 'r') as file:
            queries = file.readlines()

        # Open CSV file to write the results
        output_file = "table_counts.csv"
        with open(output_file, mode="w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["table_name", "source_count"])  # Write header

            for query in queries:
                query = query.strip()  # Remove any extra whitespace or newlines
                if query:  # Ensure query is not empty
                    try:
                        # Execute the SQL query
                        cursor.execute(query)
                        result = cursor.fetchone()
                        row_count = result[0]

                        # Extract the table name more robustly from the query
                        table_name = extract_table_name(query)

                        if table_name:
                            # Write the table name and row count to the CSV
                            writer.writerow([table_name, row_count])
                            print(
                                f"Processed table: {table_name}, Row count: {row_count}")  # Print to CMD/PyCharm console
                        else:
                            print(f"Could not extract table name from query: {query}")

                    except cx_Oracle.DatabaseError as e:
                        print(f"Error executing query: {query}, Error: {e}")

        print(f"Results written to {output_file}")

    except cx_Oracle.DatabaseError as e:
        print("There was a problem connecting to the database or executing the query:", e)

    finally:
        # Ensure resources are cleaned up
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection is not None:
            connection.close()


def extract_table_name(query):
    """Extract the table name from a query string."""
    try:
        # Convert to uppercase for case-insensitive processing
        query = query.upper()

        # Look for the 'FROM' keyword
        if "FROM" in query:
            # Extract the part after 'FROM'
            parts = query.split("FROM")[1].strip().split()
            if parts:
                table_part = parts[0]  # The first part is usually the table name
                # Remove any schema prefix (e.g., 'SYS.')
                table_name = table_part.split('.')[-1]  # Take the part after the last dot (if any)
                return table_name
        return None
    except Exception as e:
        print(f"Error extracting table name: {e}")
        return None


if __name__ == "__main__":
    get_table_counts_from_file()
