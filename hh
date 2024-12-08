import os
import socket
import smtplib
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

# Function to format the email content
def format_email(subject, body, attachment=None):
    email_content = f"Subject: {subject}\nBody: {body}"
    if attachment:
        email_content += f"\nAttachment: {attachment}"
    return email_content

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
        
        # Add the email body
        message.attach(MIMEText(body, "plain"))
        
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
        body = "Please find attached the comparison results between Oracle and Hive table counts."
        
        # Format the email content
        email_content = format_email(subject, body, compare_csv_file)
        
        # Send the email
        send_email_with_attachment(subject, email_content, compare_csv_file, mailto)
    
    except Exception as e:
        print(f"Error during execution: {e}")
