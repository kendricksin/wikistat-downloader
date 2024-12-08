# config.py
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    # Database settings
    DB_CONFIG = {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'connect_timeout': int(os.getenv('DB_TIMEOUT', '60'))
    }

    # Email settings
    EMAIL_CONFIG = {
        'sender': os.getenv('EMAIL_SENDER'),
        'app_password': os.getenv('EMAIL_APP_PASSWORD'),
        'recipient': os.getenv('EMAIL_RECIPIENT'),
        'smtp_server': os.getenv('SMTP_SERVER', 'smtp.gmail.com'),
        'smtp_port': int(os.getenv('SMTP_PORT', '587'))
    }

    # Worker settings
    MAX_DOWNLOAD_WORKERS = int(os.getenv('MAX_DOWNLOAD_WORKERS', '2'))
    MAX_IMPORT_WORKERS = int(os.getenv('MAX_IMPORT_WORKERS', '2'))
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000'))
    DOWNLOAD_RETRY_DELAY = int(os.getenv('DOWNLOAD_RETRY_DELAY', '300'))

    # Data directory settings
    DATA_DIR = Path(os.getenv('DATA_DIR', 'wikistat_data'))
    LOG_FILE = os.getenv('LOG_FILE', 'wikistat_import.log')
    
    # Dataset settings
    START_YEAR = int(os.getenv('START_YEAR', '2015'))
    START_MONTH = int(os.getenv('START_MONTH', '5'))

    # State file paths
    FAILED_DOWNLOADS_FILE = DATA_DIR / 'failed_downloads.json'
    SUCCESSFUL_IMPORTS_FILE = DATA_DIR / 'successful_imports.json'

    @classmethod
    def initialize(cls):
        """Initialize configuration and create necessary directories"""
        cls.DATA_DIR.mkdir(exist_ok=True)
        return cls

    @classmethod
    def validate_config(cls):
        """Validate that all required environment variables are set"""
        required_vars = {
            'DB_HOST': cls.DB_CONFIG['host'],
            'DB_USER': cls.DB_CONFIG['user'],
            'DB_PASSWORD': cls.DB_CONFIG['password'],
            'EMAIL_SENDER': cls.EMAIL_CONFIG['sender'],
            'EMAIL_APP_PASSWORD': cls.EMAIL_CONFIG['app_password'],
            'EMAIL_RECIPIENT': cls.EMAIL_CONFIG['recipient']
        }

        missing_vars = [var for var, value in required_vars.items() if not value]
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")