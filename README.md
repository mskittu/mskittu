- 👋 Hi, I’m @mskittu
- 👀 I’m interested in ...
- 🌱 I’m currently learning ...
- 💞️ I’m looking to collaborate on ...
- 📫 How to reach me ...
- 😄 Pronouns: ...
- ⚡ Fun fact: ...

<!---
mskittu/mskittu is a ✨ special ✨ repository because its `README.md` (this file) appears on your GitHub profile.
You can click the Preview link to take a look at your changes.
--->
#!/bin/bash

# Input and Output Files
input_file="pcds_tbl_par.txt"  # Path to the input text file
target_csv="target.csv"        # Hive output file with UNION ALL results
source_csv="source.csv"        # Oracle output file with row counts
final_csv="final.csv"          # Final comparison report
error_log="error.log"          # Log file for errors
process_log="process.log"      # Log file for process tracking

# Initialize target and source CSV files
echo "Table Name,Database Name,Row Count" > "$target_csv"
echo "Table Name,Database Name,Row Count" > "$source_csv"
echo "Processing tables from $input_file..." > "$process_log"

# Step 1: Construct Hive query with UNION ALL
hive_query=""
while IFS= read -r line; do
    table_name=$(echo "$line" | cut -d',' -f1)  # Extract table name
    db_name=$(echo "$line" | cut -d',' -f2)    # Extract database name
    condition=$(echo "$line" | cut -d',' -f3-) # Extract condition

    # Append to Hive query
    hive_query+="SELECT COUNT(*) AS count, '${table_name}' AS table_name, '${db_name}' AS db_name FROM ${db_name}.${table_name} WHERE ${condition} UNION ALL "
done < "$input_file"

# Remove the trailing 'UNION ALL'
hive_query=${hive_query%UNION ALL}

# Step 2: Execute Hive query and save results to target.csv
echo "Executing Hive query..." >> "$process_log"
hive_output=$(hive -e "$hive_query" 2>> "$error_log")

if [[ $? -ne 0 ]]; then
    echo "Error: Hive query execution failed. Check $error_log for details." >> "$process_log"
    exit 1
fi

# Save Hive results to CSV (transform output format)
echo "$hive_output" | sed 's/[[:space:]]/,/g' >> "$target_csv"
echo "Hive query results saved to $target_csv." >> "$process_log"

# Step 3: Execute Oracle queries and save results to source.csv
echo "Enter Oracle SID/Service Name..." >> "$process_log"
read -p "Oracle Service/SID: " oracle_sid

# Create an associative array to store Oracle results for quicker lookup
declare -A oracle_counts

while IFS= read -r line; do
    table_name=$(echo "$line" | cut -d',' -f1)  # Extract table name
    db_name=$(echo "$line" | cut -d',' -f2)    # Extract database name
    condition=$(echo "$line" | cut -d',' -f3-) # Extract condition

    # Construct SQL query
    sql_query="SELECT COUNT(*) AS count FROM ${db_name}.${table_name} WHERE ${condition};"

    # Execute Oracle query using environment-based authentication
    result=$(sqlplus -s "/ as sysdba" <<EOF
SET HEADING OFF;
SET FEEDBACK OFF;
SET PAGESIZE 0;
$sql_query
EXIT;
EOF
    )

    # Check for error in Oracle query execution
    if [[ -z "$result" ]]; then
        echo "Error: Oracle query failed for ${table_name} in ${db_name}" >> "$error_log"
        continue
    fi

    # Store result in associative array for quick lookup
    oracle_counts["$table_name,$db_name"]="$result"

    # Append result to source.csv
    echo "${table_name},${db_name},${result}" >> "$source_csv"
    echo "Processed table: $table_name in $db_name" >> "$process_log"
done < "$input_file"

echo "Oracle query results saved to $source_csv." >> "$process_log"

# Step 4: Compare target.csv and source.csv to create final.csv
echo "Table Name,Database Name,Hive Count,Oracle Count,Status" > "$final_csv"

# Read the target CSV file and compare with Oracle results
while IFS= read -r target_line; do
    # Skip header
    if [[ "$target_line" == "Table Name,Database Name,Row Count" ]]; then
        continue
    fi

    # Extract data from target.csv
    table_name=$(echo "$target_line" | cut -d',' -f1)
    db_name=$(echo "$target_line" | cut -d',' -f2)
    hive_count=$(echo "$target_line" | cut -d',' -f3)

    # Find corresponding line in source.csv using associative array
    oracle_count="${oracle_counts["$table_name,$db_name"]}"

    # If Oracle count is not found, log the error
    if [[ -z "$oracle_count" ]]; then
        echo "Error: No Oracle data found for ${table_name} in ${db_name}" >> "$error_log"
        status="MISSING"
    elif [[ "$hive_count" == "$oracle_count" ]]; then
        status="MATCH"
    else
        status="MISMATCH"
    fi

    # Append to final.csv
    echo "${table_name},${db_name},${hive_count},${oracle_count},${status}" >> "$final_csv"
done < "$target_csv"

echo "Comparison completed. Results saved in $final_csv." >> "$process_log"
echo "Processing completed. Check $process_log for detailed log and $error_log for errors."
