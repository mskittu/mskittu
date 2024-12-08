import os
import socket
import smtplib
import csv
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime

# Set the base paths for files
base_path = os.getenv("FILE_PATH", "/default/path")
oracle_query_file = os.path.join(base_path, "query_file.txt")
compare_csv_file = os.path.join(base_path, "compare.csv")

# Email configuration function
def environmentval():
    # Get the first 4 characters of the hostname
    edgenode_env = socket.gethostname()[:4]
    print(f"edgenode_env is {edgenode_env}")
    
    # Define mappings for known environments
    environment_mappings = {
        "dcds": ("DEV", "UAT87@mo-collab.barclayscorp.com"), 
        "qcds": ("QA", "UAT87@mo-collab.barclayscorp.com"),
        "pcds": ("PROD", "IDTeamImpairment@barclays.com, USCDSRTBTEAM@barclays.com"),
    }

    # Check if the environment is recognized, else set default
    if edgenode_env in environment_mappings:
        upper_edgenode_env, mailto = environment_mappings[edgenode_env]
    else:
        # Set default to DEV if environment is unknown
        upper_edgenode_env, mailto = "DEV", "UAT87@mo-collab.barclayscorp.com"
        print(f"Warning: Unknown environment '{edgenode_env}', using default values.")
    
    # Print environment details
    print(f"Environment: {upper_edgenode_env}")
    print(f"Mail To: {mailto}")
    return mailto

# Function to read compare.csv and generate HTML table
def generate_html_table(csv_file):
    html_content = '''
    <html>
    <head>
        <style>
            table {
                border-collapse: collapse;
                width: 100%;
            }
            th, td {
                border: 1px solid #dddddd;
                text-align: left;
                padding: 8px;
            }
            th {
                background-color: #f2f2f2;
            }
            .status-match {
                background-color: #90EE90; /* Light Green for match */
            }
            .status-mismatch {
                background-color: #FFCCCB; /* Light Red for mismatch */
            }
        </style>
    </head>
    <body>
        <h2>Oracle and Hive Table Count Comparison</h2>
        <table>
            <tr>
                <th>Table Name</th>
                <th>Oracle Count</th>
                <th>Hive Count</th>
                <th>Status</th>
            </tr>
    '''
    # Read the CSV and generate table rows
    with open(csv_file, 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            status_class = "status-match" if row['Oracle_Count'] == row['Hive_Count'] else "status-mismatch"
            html_content += f'''
            <tr>
                <td>{row['Table_Name']}</td>
                <td>{row['Oracle_Count']}</td>
                <td>{row['Hive_Count']}</td>
                <td class="{status_class}">{row['Status']}</td>
            </tr>
            '''
    
    html_content += '''
        </table>
    </body>
    </html>
    '''
    return html_content

# Function to send an email with an attachment
def send_email_with_attachment(subject, body, attachment_path, recipients):
    try:
        # Email sender configuration
        sender_email = "noreply-fin-notification@barclaycardus.com"
        smtp_server = "smtp-relay-uat.barclays.intranet"
        smtp_port = 25
        
        # Create the email
        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = recipients
        message["Subject"] = subject
        
        # Add the email body in HTML format
        message.attach(MIMEText(body, "html"))
        
        # Attach the CSV file
        with open(attachment_path, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={os.path.basename(attachment_path)}")
        message.attach(part)
        
        # Send the email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.sendmail(sender_email, recipients.split(","), message.as_string())
        print(f"Email sent successfully to {recipients}")
    
    except Exception as e:
        print(f"Error sending email: {e}")

# Main execution logic
if __name__ == "__main__":
    try:
        # Retrieve the email recipient based on the environment
        mailto = environmentval()
        
        # Get today's date in the format "YYYY-MM-DD"
        today_date = datetime.today().strftime('%Y-%m-%d')
        
        # Define the subject with today's date
        subject = f"Oracle and Hive Table Count Comparison Results_{today_date}"
        
        # Generate the HTML body from the CSV file
        body = generate_html_table(compare_csv_file)
        
        # Send the email
        send_email_with_attachment(subject, body, compare_csv_file, mailto)
    
    except Exception as e:
        print(f"Error during execution: {e}")
