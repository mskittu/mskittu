import os
import cx_Oracle
import pandas as pd
import subprocess
from pyspark.sql import SparkSession

# Initialize SparkSession
# This initializes a Spark session with Hive support for reading data from Hive tables
spark = SparkSession.builder \
    .appName("PPNR Accuracy Report") \
    .enableHiveSupport() \
    .getOrCreate()

def fetch_pcds_data():
    try:
        # Oracle connection details
        # Setting up Oracle connection details using environment variables
        db_name = "CDS"
        user_var = f"USERNAME_{db_name}"
        passwd_var = f"PWD_{db_name}"
        service_var = f"CONNECT_STRING_{db_name}"
        user_name = os.environ[user_var]
        passwd = os.environ[passwd_var]
        service_nm = os.environ[service_var]

        # Parse the service string
        # Extract host, port, and service name from the connection string
        service_nm = service_nm[service_nm.index('@') + 1:]
        host = service_nm[0:service_nm.index(':')]
        port = service_nm[service_nm.index(':') + 1:service_nm.index('/')]
        service_nm = service_nm[service_nm.index('/') + 1:]

        # Create a DSN (Data Source Name)
        dsn = cx_Oracle.makedsn(host, port, service_name=service_nm)

        # Establish a connection
        connection = cx_Oracle.connect(user=user_name, password=passwd, dsn=dsn)
        cursor = connection.cursor()

        # Hardcoded query for PPNR attribute
        query = """
        SELECT account_id, fcfee_chrgoff_rvrsl_amt_src
        FROM asp_eom_acct_sum
        WHERE ROWNUM <= 1000
        """

        # Execute the query
        cursor.execute(query)
        rows = cursor.fetchall()

        # Convert the result to a DataFrame
        columns = [desc[0] for desc in cursor.description]
        data_df = pd.DataFrame(rows, columns=columns)

        # Close the connection
        cursor.close()
        connection.close()

        return data_df

    except Exception as e:
        # Handle and print any errors encountered
        print(f"Error while fetching data from PCDS: {e}")
        return None

def fetch_and_compare_data(target_table, key_columns):
    # Fetch data from PCDS
    source_data = fetch_pcds_data()
    source_df = spark.createDataFrame(source_data)

    # Fetch data from Hive
    output_file = "/tmp/hive_output.csv"
    hive_command = 'hive -e "SELECT account_id, fcfee_chrgoff_rvrsl_amt_src FROM cds_asp_eom_acct_sum;" > /tmp/hive_output.csv'
    subprocess.run(hive_command, shell=True, check=True)

    # Read Hive query result into a DataFrame
    target_df = spark.read.csv(output_file, header=True, inferSchema=True)

    # Select 1000 accounts for comparison
    sample_source_df = source_df.limit(1000)
    sample_target_df = target_df.limit(1000)

    # Compare data
    mismatches = sample_source_df.join(sample_target_df, key_columns, how="outer") \
        .filter((sample_source_df[key_columns[0]].isNull()) | (sample_target_df[key_columns[0]].isNull()))

    return mismatches

def generate_accuracy_report(mismatches, total_records):
    # Calculate mismatch count and accuracy
    mismatch_count = mismatches.count()
    accuracy = (1 - mismatch_count / total_records) * 100

    # Write accuracy report to a file
    accuracy_report_path = "ppnr_accuracy_report.csv"
    with open(accuracy_report_path, "a") as report_file:
        report_file.write(f"Accuracy for PPNR: {accuracy:.2f}%\n")

    # Print accuracy result to console
    print(f"Accuracy for PPNR: {accuracy:.2f}% written to {accuracy_report_path}")

# Define source and target tables and attributes to compare
# Hardcoded Hive table and key columns for PPNR
hive_table = "cds_asp_eom_acct_sum"
key_columns = ["account_id"]

# Perform comparisons and generate accuracy reports
mismatches = fetch_and_compare_data(hive_table, key_columns)
generate_accuracy_report(mismatches, 1000)

# Stop Spark session
# Terminate the Spark session
spark.stop()
