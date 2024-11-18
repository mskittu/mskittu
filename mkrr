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

# Display the content of the input file (cat command)
echo "Processing file: $QUERY_FILE"
cat "$QUERY_FILE"

# Clear the output file and add headers
echo "Table Name, Database Name, Row Count" > "$SOURCE_OUTPUT_FILE"

# Read and process each line in the input file
cat "$QUERY_FILE" | while IFS=',' read -r table_name db_name schema_name condition; do
    # Trim spaces from variables
    table_name=$(echo "$table_name" | xargs)
    db_name=$(echo "$db_name" | xargs)
    schema_name=$(echo "$schema_name" | xargs)
    condition=$(echo "$condition" | xargs)

    # Check for valid inputs
    if [[ -z "$table_name" || -z "$db_name" || -z "$schema_name" || -z "$condition" ]]; then
        echo "Skipping invalid line: $table_name, $db_name, $schema_name, $condition"
        continue
    fi

    # Construct the SQL query
    sql_query="SELECT COUNT(*) FROM $schema_name WHERE $condition;"

    # Log the query being executed
    echo "Executing query for table $table_name in database $db_name: $sql_query"

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

    # Check for errors or empty results
    if [[ -z "$row_count" || "$row_count" == *"ORA-"* ]]; then
        echo "Error executing query for $table_name in $db_name. Skipping."
        row_count="ERROR"
    fi

    # Write the result to the output file
    echo "$table_name, $db_name, $row_count" >> "$SOURCE_OUTPUT_FILE"

done

echo "Processing complete. Results saved to '$SOURCE_OUTPUT_FILE'."
