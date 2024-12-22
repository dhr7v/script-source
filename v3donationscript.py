import os
import shutil
import pandas as pd
import PyPDF2
import logging
from typing import Optional, List, Tuple, Dict
import re
from multiprocessing import cpu_count
from functools import partial
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import configparser
import concurrent.futures
import time
from datetime import datetime
from ratelimit import limits, sleep_and_retry

log_filename = datetime.now().strftime("logfile_%Y%m%d_%H%M%S.txt")

# Set up logging to file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
def extract_pan_from_pdf(pdf_path: str) -> Optional[str]:
    """Extract the Unique Identification Number (PAN) from a PDF file."""
    try:
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            text = "".join(page.extract_text() for page in reader.pages)

        match = re.search(r'Unique Identification Number\s+([A-Z]{5}[0-9]{4}[A-Z])', text)
        if match:
            return match.group(1)
        else:
            logger.warning(f"Could not find Unique Identification Number in {pdf_path}")
            return None
    except Exception as e:
        logger.error(f"Error extracting PAN from {pdf_path}: {str(e)}")
        return None

def get_email_for_pan(pan: str, df: pd.DataFrame) -> Optional[str]:
    """Get email address for a given PAN from the dataframe."""
    try:
        email_records = df[df['PAN'] == pan]
        return email_records.iloc[0]['eMail ID'] if not email_records.empty else None
    except Exception as e:
        logger.error(f"Error getting email for PAN {pan}: {str(e)}")
        return None

def process_pdf(pdf_file: str, pdf_dir: str, output_dir: str, df: pd.DataFrame) -> Optional[Tuple[str, str, str]]:
    """
    Process a single PDF file: extract PAN and prepare for grouping.
    
    Returns:
        Optional[Tuple[str, str, str]]: Tuple of (PAN, email, pdf_path) if successful, None otherwise.
    """
    try:
        pdf_path = os.path.join(pdf_dir, pdf_file)
        pan = extract_pan_from_pdf(pdf_path)

        if pan:
            email = get_email_for_pan(pan, df)
            if email:
                pan_dir = os.path.join(output_dir, pan)
                os.makedirs(pan_dir, exist_ok=True)
                output_path = os.path.join(pan_dir, pdf_file)
                shutil.copy(pdf_path, output_path)
                return (pan, email, output_path)
            else:
                logger.warning(f"No email found for PAN: {pan}")
        else:
            logger.warning(f"Could not extract PAN from {pdf_file}")

        return None
    except Exception as e:
        logger.error(f"Error processing {pdf_file}: {str(e)}")
        return None

CALLS_PER_MINUTE = 20
PERIOD = 60
MAX_RETRIES = 5
BASE_DELAY = 1

@sleep_and_retry
@limits(calls=CALLS_PER_MINUTE, period=PERIOD)
def send_grouped_email_with_retry(email: str, attachments: List[str], email_config: dict) -> bool:
    """Send an email with multiple attachments, using rate limiting and exponential backoff."""
    for attempt in range(MAX_RETRIES):
        try:
            msg = MIMEMultipart()
            msg['From'] = email_config['sender_email']
            msg['To'] = email
            msg['Subject'] = email_config['subject']

            msg.attach(MIMEText(email_config['body'], 'plain'))

            # Attach all PDFs for this email
            for file_path in attachments:
                with open(file_path, "rb") as f:
                    part = MIMEApplication(f.read(), Name=os.path.basename(file_path))
                part['Content-Disposition'] = f'attachment; filename="{os.path.basename(file_path)}"'
                msg.attach(part)

            with smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port']) as server:
                server.starttls()
                server.login(email_config['sender_email'], email_config['sender_password'])
                server.send_message(msg)

            logger.info(f"Email sent successfully to {email} with {len(attachments)} attachments")
            return True
        except Exception as e:
            delay = BASE_DELAY * (2 ** attempt)
            logger.warning(f"Attempt {attempt + 1} failed to send email to {email}. Retrying in {delay} seconds. Error: {str(e)}")
            time.sleep(delay)

    logger.error(f"Failed to send email to {email} after {MAX_RETRIES} attempts.")
    return False

def main():
    try:
        # Load configuration
        config = configparser.ConfigParser()
        config.read('config.properties')

        # Load the table data
        df = pd.read_csv(config['Directories']['table_file'])
        logger.info(f"Loaded data from {config['Directories']['table_file']}")

        # Directory paths
        pdf_dir = config['Directories']['pdf_directory']
        grouped_dir = config['Directories']['grouped_directory']
        processed_dir = config['Directories']['processed_directory']

        # Ensure directories exist
        os.makedirs(grouped_dir, exist_ok=True)
        os.makedirs(processed_dir, exist_ok=True)

        # Get list of PDF files
        pdf_files = [f for f in os.listdir(pdf_dir) if f.lower().endswith('.pdf')]

        # Process PDFs in parallel and collect results
        with concurrent.futures.ThreadPoolExecutor(max_workers=cpu_count() * 2) as executor:
            process_pdf_partial = partial(
                process_pdf,
                pdf_dir=pdf_dir,
                output_dir=grouped_dir,
                df=df
            )
            results = list(executor.map(process_pdf_partial, pdf_files))

        # Group PDFs by email
        email_groups: Dict[str, List[str]] = {}
        pan_to_files: Dict[str, List[str]] = {}
        
        for result in results:
            if result:
                pan, email, pdf_path = result
                if email not in email_groups:
                    email_groups[email] = []
                if pan not in pan_to_files:
                    pan_to_files[pan] = []
                    
                email_groups[email].append(pdf_path)
                pan_to_files[pan].append(pdf_path)

        # Prepare email configuration
        email_config = {
            'sender_email': config['Email']['sender_email'],
            'sender_password': config['Email']['sender_password'],
            'smtp_server': config['Email']['smtp_server'],
            'smtp_port': config['Email'].getint('smtp_port'),
            'subject': config['Email']['subject'],
            'body': config['Email']['body'].replace('<br>', '\n')
        }

        # Send grouped emails and move to processed directory
        for email, attachments in email_groups.items():
            if send_grouped_email_with_retry(email, attachments, email_config):
                # Move all successfully sent files to processed directory
                for pdf_path in attachments:
                    pdf_name = os.path.basename(pdf_path)
                    pan = next(pan for pan, files in pan_to_files.items() if pdf_path in files)
                    processed_pan_dir = os.path.join(processed_dir, pan)
                    os.makedirs(processed_pan_dir, exist_ok=True)
                    shutil.move(pdf_path, os.path.join(processed_pan_dir, pdf_name))

        logger.info(f"Finished processing PDFs. Sent {len(email_groups)} emails with grouped attachments.")
        logger.info("Script execution completed successfully")

    except Exception as e:
        logger.error(f"An error occurred during script execution: {str(e)}")

if __name__ == "__main__":
    main()