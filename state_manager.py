# state_manager.py
import json
import logging
from config import Config

class StateManager:
    def __init__(self):
        # Initialize Config if not already initialized
        Config.initialize()
        
        self.failed_downloads_file = Config.FAILED_DOWNLOADS_FILE
        self.successful_imports_file = Config.SUCCESSFUL_IMPORTS_FILE
        self.logger = logging.getLogger(__name__)
        
    def load_state(self):
        failed_downloads = set()
        successful_imports = set()
        
        try:
            if self.failed_downloads_file.exists():
                with open(self.failed_downloads_file, 'r') as f:
                    failed_downloads = set(tuple(x) for x in json.load(f))
            
            if self.successful_imports_file.exists():
                with open(self.successful_imports_file, 'r') as f:
                    successful_imports = set(tuple(x) for x in json.load(f))
                    
        except Exception as e:
            self.logger.error(f"Error loading state: {e}")
            
        return failed_downloads, successful_imports
        
    def save_state(self, failed_downloads, successful_imports):
        try:
            with open(self.failed_downloads_file, 'w') as f:
                json.dump(list(failed_downloads), f)
            
            with open(self.successful_imports_file, 'w') as f:
                json.dump(list(successful_imports), f)
        except Exception as e:
            self.logger.error(f"Error saving state: {e}")