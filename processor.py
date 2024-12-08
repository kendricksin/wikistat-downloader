# processor.py - Data processing logic
import gzip
from urllib.parse import unquote
import logging

class DataProcessor:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def process_line(self, line):
        """Process a single line of WikiStat data"""
        try:
            parts = line.strip().split(' ')
            if len(parts) < 4:
                return None
                
            project_parts = parts[0].split('.')
            project = project_parts[0]
            subproject = project_parts[1] if len(project_parts) > 1 else None
            
            path = unquote(parts[1])
            hits = int(parts[2])
            
            return (project, subproject, path, hits)
        except Exception:
            return None

    def process_file(self, file_path, timestamp):
        """Generator to yield processed records"""
        try:
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                for line in f:
                    processed = self.process_line(line)
                    if processed:
                        project, subproject, path, hits = processed
                        yield (timestamp, project, subproject, path, hits)
        except Exception as e:
            self.logger.error(f"Error processing file {file_path}: {e}")