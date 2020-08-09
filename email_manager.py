import smtplib, ssl
from credentials import email_address, password
from email import encoders
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
import pandas as pd

from logging_config import get_logger
from utils import set_indexes

email_manager_logger = get_logger("ark.email_manager")

class EmailManager:

    def __init__(self, name, csv_filename):
        email_manager_logger.info('starting __init__ for EmailManager')
        self.name = name
        self.port = 465  # For SSL
        self.smtp_server = "smtp.gmail.com"
        self.sender = email_address
        self.password = password
        self.csv_filename = csv_filename
        self.contact_df = set_indexes(pd.read_csv('data_tables/contact_df.csv'))
        self.rec_email = self.contact_df.loc[self.name, 'Contact: Email address']

    def create_message(self):
        try:
            email_manager_logger.info("starting create_message")
            message = MIMEMultipart("alternative")
            message["Subject"] = "ARK survey response"
            message["From"] = self.sender
            message["To"] = self.rec_email

            email_manager_logger.info("writing plain text for email")
            text = """\
            Hi {},

            Thank you for using ARK! Please see your results attached.
            Please send any queries to {}.

            Many thanks,

            Team Ark""".format(self.name, self.sender)

            email_manager_logger.info("writing html text for email")
            html = """\
            <html>
              <body>
                <p>Hi {},
                <br>
                   Thank you for using ARK! Please see your results attached.
                </br>
                <br>
                    Please send any queries to {}.
                </br>
                <br>
                    Many thanks,
                </br>
                <br>
                    Team Ark
                </br>
                </p>
              </body>
            </html>
            """.format(self.name, self.sender)

            email_manager_logger.info("setting text and html parts and attaching to message")
            text_part = MIMEText(text, "plain")
            html_part = MIMEText(html, "html")

            # Add HTML/plain-text parts to MIMEMultipart message
            # The email client will try to render the last part first
            message.attach(text_part)
            message.attach(html_part)

            email_manager_logger.info("attaching csv to email")
            with open(self.csv_filename, "rb") as attachment:
                # Add file as application/octet-stream
                # Email client can usually download this automatically as attachment
                attachment_part = MIMEBase("application", "octet-stream")
                attachment_part.set_payload(attachment.read())

            email_manager_logger.info("encoding file in ASCII")
            # Encode file in ASCII characters to send by email
            encoders.encode_base64(attachment_part)

            email_manager_logger.info("adding header to attachement")
            # Add header as key/value pair to attachment part
            attachment_part.add_header(
                "Content-Disposition",
                "attachment; filename= {}".format(self.csv_filename.split('/')[-1]),
            )

            email_manager_logger.info("finishing create_message")
            # Add attachment to message and convert message to string
            message.attach(attachment_part)
            message_as_string = message.as_string()

            return message_as_string
        except Exception as e:
            email_manager_logger.error("Error in create_message: {}".format(e))
            raise e

    def send_message(self):

        email_manager_logger.info("running send_message")

        try:
            context = ssl.create_default_context()

            message_as_string = self.create_message()

            with smtplib.SMTP_SSL(self.smtp_server, self.port, context=context) as server:
                server.login(self.sender, self.password)
                server.sendmail(self.sender,  self.rec_email, message_as_string)

            email_manager_logger.info("message sent")
        except Exception as e:
            email_manager_logger.error("Error in send_message: {}".format(e))
            raise e
