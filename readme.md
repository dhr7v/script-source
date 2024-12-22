PDF Mailer service (Created for Form 10BE Donation Tax receipt, easily changeable- I have made a different version for form 16s very easily using this base script)

PDFs go in /pdfs/all-pdfs/ directory.
Donations.csv has the email addresses for each Personal identification number.
(there can be multiple pdfs for each record)

What it does-

It:
-Groups pdfs by parsing PAN number from documents,
-Moves to a grouped folder, emails each group as attachments,
-Moves each group to the processed folder

There is a rate limit counter in case of failure,
There is extensive logging in the CLI and saved as a log file for each session for completion tracking, although there is a final counter at the end of the run.
Directories, csv file, email sender credentials, and email content can be changed in config. properties


Run :
python v3donationscript.py



