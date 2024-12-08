# downloader.py
import requests
import backoff
import logging
import time
import re
from pathlib import Path
from bs4 import BeautifulSoup
from datetime import datetime
from config import Config

class Downloader:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.data_dir = Config.DATA_DIR
        self.data_dir.mkdir(exist_ok=True)
        self._last_download_time = 0
        self._min_delay = 3.0  # Increased from 1.0 to 3.0 seconds
        self._consecutive_failures = 0
        self._backoff_time = 60  # Initial backoff time in seconds
        self._max_backoff_time = 900  # Maximum backoff time (15 minutes)
        
        # Configure session with longer timeouts and keep-alive
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WikiStat Data Importer/1.0 (your-email@example.com)',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        })
        adapter = requests.adapters.HTTPAdapter(
            max_retries=3,
            pool_connections=3,
            pool_maxsize=3,
            pool_block=True
        )
        self.session.mount('https://', adapter)
        
        # Regular expression for parsing pageview filenames
        self.filename_pattern = re.compile(r'pageviews-(\d{4})(\d{2})(\d{2})-(\d{2})0000\.gz$')

    def _handle_download_failure(self):
        """Handle failed downloads with exponential backoff"""
        self._consecutive_failures += 1
        if self._consecutive_failures > 3:
            # Exponential backoff with jitter
            backoff_time = min(self._backoff_time * (2 ** (self._consecutive_failures - 3)), self._max_backoff_time)
            jitter = backoff_time * 0.1  # 10% jitter
            sleep_time = backoff_time + (time.random() * jitter)
            self.logger.warning(f"Multiple download failures. Backing off for {sleep_time:.1f} seconds")
            time.sleep(sleep_time)
    
    def _handle_download_success(self):
        """Reset failure counters after successful download"""
        self._consecutive_failures = 0
        self._backoff_time = 60

    def _wait_for_rate_limit(self):
        """Ensure minimum delay between downloads with adaptive timing"""
        now = time.time()
        elapsed = now - self._last_download_time
        
        # Adjust delay based on consecutive failures
        current_delay = self._min_delay * (1.5 ** min(self._consecutive_failures, 3))
        
        if elapsed < current_delay:
            sleep_time = current_delay - elapsed
            time.sleep(sleep_time)
        
        self._last_download_time = time.time()

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException, IOError),
        max_tries=5,  # Increased from 3 to 5
        max_time=300,  # 5 minutes maximum retry time
        giveup=lambda e: (
            isinstance(e, requests.exceptions.HTTPError) and 
            e.response is not None and 
            e.response.status_code in [404, 403]  # Don't retry on 404 or 403
        )
    )
    def download_file(self, year, month, day, hour):
        """Download with improved retry logic and rate limiting"""
        filename = f"pageviews-{year}{month:02d}{day:02d}-{hour:02d}0000.gz"
        url = f"https://dumps.wikimedia.org/other/pageviews/{year}/{year}-{month:02d}/{filename}"
        file_path = self.data_dir / filename
        
        if file_path.exists():
            self.logger.info(f"File already exists: {file_path}")
            return file_path
            
        try:
            self._wait_for_rate_limit()
            
            self.logger.info(f"Downloading {url}")
            response = self.session.get(
                url,
                stream=True,
                timeout=60,  # Increased timeout
                allow_redirects=True
            )
            response.raise_for_status()
            
            # Stream the file to disk with progress tracking
            total_size = int(response.headers.get('content-length', 0))
            block_size = 8192
            wrote = 0
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=block_size):
                    if chunk:
                        wrote += len(chunk)
                        f.write(chunk)
            
            if total_size > 0 and wrote != total_size:
                raise IOError(f"Downloaded file size mismatch: {wrote} vs {total_size}")
            
            self._handle_download_success()
            self.logger.info(f"Successfully downloaded: {file_path}")
            return file_path
            
        except Exception as e:
            self.logger.error(f"Error downloading file {url}: {e}")
            self._handle_download_failure()
            if file_path.exists():
                file_path.unlink()  # Remove partial downloads
            raise