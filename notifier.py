# notifier.py - Email notification handling
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging
from config import Config

class EmailNotifier:
    def __init__(self):
        self.config = Config.EMAIL_CONFIG
        self.logger = logging.getLogger(__name__)

    def send_notification(self, subject, body):
        try:
            msg = MIMEMultipart()
            msg['From'] = self.config['sender']
            msg['To'] = self.config['recipient']
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))
            
            server = smtplib.SMTP(self.config['smtp_server'], self.config['smtp_port'])
            server.starttls()
            server.login(self.config['sender'], self.config['app_password'])
            server.send_message(msg)
            server.quit()
            
            self.logger.info(f"Email notification sent: {subject}")
        except Exception as e:
            self.logger.error(f"Failed to send email: {e}")