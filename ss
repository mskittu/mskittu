#!/bin/bash

# Define the input and output file paths
JOB_CONF="SJOB_CONF"
QUERY_FILE="$JOB_CONF/pcds_tbl_par.txt"
SOURCE_OUTPUT_FILE="STFILE_PATH/source_count.csv"

# Check if the input file exists
if [ ! -f "$QUERY_FILE" ]; then
    echo "Error: Input file '$QUERY_FILE' does not exist!"
    exit 1
fi

# Clear the output file and add headers
echo "Portfolio Type, Table Name, Database Name, Row Count" > "$SOURCE_OUTPUT_FILE"

# Read and process each line in the input file
cat "$QUERY_FILE" | while read -r file; do
    # Extract table details
    tbl_dtls=$(echo "$file" | cut -d'.' -f1)
    # Extract portfolio type
    portfolio_type=$(echo "$file" | cut -d'_' -f1)

    # Split the line into tablename, dbname, and condition
    tablename=$(echo "$file" | awk -F',' '{print $1}' | xargs)
    dbname=$(echo "$file" | awk -F',' '{print $2}' | xargs)
    condition=$(echo "$file" | awk -F',' '{print $3}' | xargs)

    # Skip invalid or malformed lines
    if [[ -z "$tablename" || -z "$dbname" || -z "$condition" ]]; then
        echo "Skipping invalid line: $file"
        continue
    fi

    # Construct the SQL query
    sql_query="SELECT COUNT(*) FROM $dbname.$tablename WHERE $condition;"

    # Execute the query using sqlplus and capture the result
    row_count=$(sqlplus -s /nolog <<EOF
    CONNECT / AS SYSDBA
    SET FEEDBACK OFF
    SET HEADING OFF
    SET PAGESIZE 0
    SET TRIMSPOOL ON
    $sql_query
    EXIT
EOF
    )

    # Handle possible errors or empty results from the query
    if [[ -z "$row_count" || "$row_count" == *"ORA-"* ]]; then
        echo "Error executing query for $tablename in $dbname. Skipping."
        row_count="ERROR"
    fi

    # Write the result to the output file
    echo "$portfolio_type, $tablename, $dbname, $row_count" >> "$SOURCE_OUTPUT_FILE"
done

echo "Processing complete. Results saved to '$SOURCE_OUTPUT_FILE'."
