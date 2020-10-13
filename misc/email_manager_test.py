import smtplib, ssl
from credentials import email_address, password
from email import encoders
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase


port = 465  # For SSL
smtp_server = "smtp.gmail.com"
sender_email = email_address
receiver_email = email_address

message = MIMEMultipart("alternative")
message["Subject"] = "multipart test"
message["From"] = sender_email
message["To"] = receiver_email

# Create the plain-text and HTML version of your message
text = """\
Hi,
How are you?
Real Python has many great tutorials:
www.realpython.com"""
html = """\
<html>
  <body>
    <p>Hi,<br>
       How are you?<br>
       <a href="http://www.realpython.com">Real Python</a>
       has many great tutorials.
    </p>
  </body>
</html>
"""

# Turn these into plain/html MIMEText objects
part1 = MIMEText(text, "plain")
part2 = MIMEText(html, "html")

# Add HTML/plain-text parts to MIMEMultipart message
# The email client will try to render the last part first
message.attach(part1)
message.attach(part2)

filename = "results/single_test_08_09_2020.csv"  # In same directory as script


# Open CSV file in binary mode
with open(filename, "rb") as attachment:
    # Add file as application/octet-stream
    # Email client can usually download this automatically as attachment
    attachment_part = MIMEBase("application", "octet-stream")
    attachment_part.set_payload(attachment.read())


# Encode file in ASCII characters to send by email
encoders.encode_base64(attachment_part)

# Add header as key/value pair to attachment part
attachment_part.add_header(
    "Content-Disposition",
    "attachment; filename= {}".format(filename.split('/')[-1]),
)

# Add attachment to message and convert message to string
message.attach(attachment_part)
text = message.as_string()

# Create secure connection with server and send email
context = ssl.create_default_context()

with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
    server.login(sender_email, password)
    server.sendmail(sender_email, receiver_email, text)
