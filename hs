import os
import csv
import cx_Oracle
import subprocess

oracle_query_file = '/apps/tenant_local/usrf_stg/HadoopSandbox/x01532741/job/fin_recon/mds_daily_tbl_recon/0.9/conf/query_file.txt'
compare_csv_file = '/apps/tenant_local/usrf_stg/HadoopSandbox/x01532741/job/fin_recon/mds_daily_tbl_recon/0.9/source/compare.csv'

def get_oracle_table_counts_from_file(query_file):
    try:
        # Oracle connection details
        db_name = "CDS"
        user_var = f"USERNAME_{db_name}"
        passwd_var = f"PWD_{db_name}"
        service_var = f"CONNECT_STRING_{db_name}"
        user_name = os.environ[user_var]
        passwd = os.environ[passwd_var]
        service_nm = os.environ[service_var]

        # Parse the service string
        service_nm = service_nm[service_nm.index('@') + 1:]
        host = service_nm[0:service_nm.index(':')]
        port = service_nm[service_nm.index(':') + 1:service_nm.index('/')]
        service_nm = service_nm[service_nm.index('/') + 1:]

        # Create a DSN (Data Source Name)
        dsn = cx_Oracle.makedsn(host, port, service_name=service_nm)

        # Establish a connection
        connection = cx_Oracle.connect(user=user_name, password=passwd, dsn=dsn)
        cursor = connection.cursor()

        # Read queries from the provided file
        with open(query_file, 'r') as file:
            queries = file.readlines()

        results = []
        for query in queries:
            query = query.strip().rstrip(';')
            if query:
                table_name = query.split("FROM")[1].strip()
                print(f"Executing Oracle query: {query}")
                cursor.execute(query)
                result = cursor.fetchone()
                results.append((table_name, result[0]))

        return results

    except cx_Oracle.DatabaseError as e:
        print("Error connecting to Oracle database or executing queries:", e)
        return None

    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection is not None:
            connection.close()

def get_hive_table_count():
    try:
        # Run Hive query to get table counts
        output_file = "/apps/tenant_local/usrf_stg/HadoopSandbox/x01532741/job/fin_recon/mds_daily_tbl_recon/0.9/source/target_count.csv"

        # Hive command
        hive_command = (
            'hive -e "SELECT \'acct_tbal_prcng_b_eoc_fact\' AS table_name, COUNT(*) AS count '
            'FROM usrf_ingest.cds_acct_tbal_prcng_b_eoc_fact '
            'UNION ALL '
            'SELECT \'acct_tbal_prcng_c_eoc_fact\' AS table_name, COUNT(*) AS count '
            'FROM usrf_ingest.cds_acct_thal_prcng_c_eoc_fact;" > {}'.format(output_file)
        )

        # Execute the Hive command
        subprocess.run(hive_command, shell=True, check=True)

        # Read results from the output file
        results = []
        with open(output_file, 'r') as file:
            for line in file.readlines():
                table_name, count = line.strip().split('\t')
                results.append((table_name, int(count)))

        return results

    except subprocess.CalledProcessError as e:
        print("Error executing Hive query:", e)
        return None

def compare_counts(oracle_counts, hive_counts):
    try:
        with open(compare_csv_file, mode='a', newline='') as csv_file:
            writer = csv.writer(csv_file)
            
            # Write the header if the file is empty
            if csv_file.tell() == 0:
                writer.writerow(["Table_Name", "Oracle_Count", "Hive_Count", "Status"])

            mismatches = []
            for oracle_table, oracle_count in oracle_counts:
                match = next((hc for hc in hive_counts if hc[0] == oracle_table), None)
                if match:
                    hive_table, hive_count = match
                    if oracle_count != hive_count:
                        mismatches.append((oracle_table, oracle_count, hive_count, "Mismatch"))
                        writer.writerow([oracle_table, oracle_count, hive_count, "Mismatch"])
                    else:
                        writer.writerow([oracle_table, oracle_count, hive_count, "Match"])
                else:
                    mismatches.append((oracle_table, oracle_count, None, "Hive Table Missing"))
                    writer.writerow([oracle_table, oracle_count, None, "Hive Table Missing"])

            if not mismatches:
                print("Counts match between Oracle and Hive for all tables.")
            else:
                print("Mismatch found. Details appended to compare.csv.")

    except Exception as e:
        print("Error during comparison:", e)

if __name__ == "__main__":
    # Retrieve counts from Oracle and Hive
    oracle_counts = get_oracle_table_counts_from_file(oracle_query_file)
    hive_counts = get_hive_table_count()

    # Compare counts and log the results
    if oracle_counts and hive_counts:
        compare_counts(oracle_counts, hive_counts)
