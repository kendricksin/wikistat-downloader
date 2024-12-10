# main.py
import logging
import threading
import queue
from datetime import datetime
import calendar
import time
from config import Config
from database import DatabaseManager
from downloader import Downloader
from processor import DataProcessor
from notifier import EmailNotifier
from state_manager import StateManager
from monitor import WorkerMonitor
from email_batch_manager import EmailBatchManager

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(Config.LOG_FILE),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

class WikistatImporter:
    def __init__(self):
        # Initialize logger first
        self.logger = logging.getLogger(__name__)
        
        # Initialize counters first
        self._processed_count = 0
        self._total_datasets = 0
        self._lock = threading.Lock()
        
        # Initialize components
        self.db_manager = DatabaseManager()
        self.downloader = Downloader()
        self.processor = DataProcessor()
        self.notifier = EmailNotifier()
        self.state_manager = StateManager()
        self.worker_monitor = WorkerMonitor()
        self.email_batch_manager = EmailBatchManager(self.notifier)
        
        # Initialize queues
        self.download_queue = queue.Queue()
        self.import_queue = queue.Queue()
        self.failed_downloads, self.successful_imports = self.state_manager.load_state()

        # Add separate events for process control
        self.download_complete = threading.Event()
        self.import_complete = threading.Event()
        
        # Now set the total_datasets after initialization
        self.email_batch_manager.total_datasets = self._total_datasets
        self.pending_imports = set()

    def _initialize_tasks(self):
        """Initialize tasks based on what process is active"""
        tasks = []
        
        if Config.MAX_DOWNLOAD_WORKERS > 0:
            # Generate download tasks only if downloading is enabled
            tasks = self.downloader.generate_download_tasks()
            self._total_datasets = len(tasks)
            self.email_batch_manager.total_datasets = self._total_datasets
            self.logger.info(f"Found {self._total_datasets} datasets to download")
            
            # Queue download tasks
            for task in tasks:
                if task not in self.successful_imports:
                    self.download_queue.put(task)
        
        elif Config.MAX_IMPORT_WORKERS > 0:
            # If only importing, scan the data directory for files to import
            self.logger.info("Scanning data directory for files to import...")
            for file_path in Config.DATA_DIR.glob('pageviews-*.gz'):
                try:
                    # Parse filename to get components
                    match = self.downloader.filename_pattern.match(file_path.name)
                    if match:
                        year, month, day, hour = map(int, match.groups())
                        if (year, month, day, hour) not in self.successful_imports:
                            self.import_queue.put((year, month, day, hour, file_path))
                            self.pending_imports.add((year, month, day, hour))
                except Exception as e:
                    self.logger.error(f"Error parsing filename {file_path}: {e}")
            
            self._total_datasets = len(self.pending_imports)
            self.email_batch_manager.total_datasets = self._total_datasets
            self.logger.info(f"Found {self._total_datasets} datasets to import")

        return tasks
    def _generate_dataset_list(self):
        current_date = datetime.now()
        datasets = []
        
        for year in range(Config.START_YEAR, current_date.year + 1):
            start_month = Config.START_MONTH if year == Config.START_YEAR else 1
            end_month = 12 if year < current_date.year else current_date.month
            
            for month in range(start_month, end_month + 1):
                _, days = calendar.monthrange(year, month)
                for day in range(1, days + 1):
                    if (year, month, day) not in self.successful_imports:
                        datasets.append((year, month, day))
        
        return datasets

    def _download_manager(self, datasets):
        """Manage the download process"""
        try:
            if Config.MAX_DOWNLOAD_WORKERS > 0:
                # Start download workers
                download_threads = []
                for i in range(Config.MAX_DOWNLOAD_WORKERS):
                    t = threading.Thread(target=self._download_worker)
                    t.daemon = True
                    t.start()
                    download_threads.append(t)
                
                # Add datasets to download queue
                for dataset in datasets:
                    self.download_queue.put(dataset)
                
                # Add poison pills for download workers
                for _ in range(Config.MAX_DOWNLOAD_WORKERS):
                    self.download_queue.put(None)
                
                # Wait for downloads to complete
                self.download_queue.join()
                
                # Wait for all download threads to finish
                for t in download_threads:
                    t.join()
            
            # Signal that downloading is complete
            self.download_complete.set()
            
        except Exception as e:
            self.logger.error(f"Error in download manager: {e}", exc_info=True)
            # Ensure import process knows downloading failed
            self.download_complete.set()
            raise

    def _import_manager(self):
        """Manage the import process"""
        try:
            if Config.MAX_IMPORT_WORKERS > 0:
                # Start import workers
                import_threads = []
                for i in range(Config.MAX_IMPORT_WORKERS):
                    t = threading.Thread(target=self._import_worker)
                    t.daemon = True
                    t.start()
                    import_threads.append(t)
                
                # Wait until either:
                # 1. All downloads are complete and import queue is empty
                # 2. Or downloading failed and import queue is empty
                while not self.download_complete.is_set() or not self.import_queue.empty():
                    time.sleep(1)
                
                # Add poison pills for import workers
                for _ in range(Config.MAX_IMPORT_WORKERS):
                    self.import_queue.put(None)
                
                # Wait for imports to complete
                self.import_queue.join()
                
                # Wait for all import threads to finish
                for t in import_threads:
                    t.join()
            
            # Signal that importing is complete
            self.import_complete.set()
            
        except Exception as e:
            self.logger.error(f"Error in import manager: {e}", exc_info=True)
            self.import_complete.set()
            raise
    def _download_worker(self):
            """Worker function for downloading datasets"""
            while True:
                try:
                    task = self.download_queue.get()
                    if task is None:  # Poison pill
                        self.download_queue.task_done()
                        break
                    
                    year, month, day, hour = task
                    self.logger.info(f"Downloading dataset {year}-{month:02d}-{day:02d} hour {hour:02d}")
                    
                    file_path = self.downloader.download_file(year, month, day, hour)
                    if file_path:
                        self.import_queue.put((year, month, day, hour, file_path))
                    else:
                        with self._lock:
                            self.failed_downloads.add((year, month, day, hour))
                            self.state_manager.save_state(self.failed_downloads, self.successful_imports)
                    
                    self.download_queue.task_done()
                    
                except Exception as e:
                    self.logger.error(f"Error in download worker: {e}")
                    if 'task' in locals():
                        with self._lock:
                            self.failed_downloads.add(task)
                            self.state_manager.save_state(self.failed_downloads, self.successful_imports)
                    self.download_queue.task_done()

    def _import_worker(self):
        """Worker function for importing datasets with improved database handling and monitoring"""
        worker_id = threading.get_ident()
        connection = None
        try:
            connection = self.db_manager.get_connection()
            
            if not self.db_manager.verify_connection(connection):
                self.logger.error("Database verification failed - recreating schema")
                self.db_manager.create_schema(connection)
                if not self.db_manager.verify_connection(connection):
                    raise Exception("Failed to create and verify database schema")
            
            while True:
                dataset = None
                try:
                    dataset = self.import_queue.get()
                    if dataset is None:  # Poison pill
                        self.worker_monitor.update_worker_status(worker_id, None, 0)
                        break
                    
                    year, month, day, hour, file_path = dataset  # Now unpacking 5 values
                    if (year, month, day, hour) in self.successful_imports:
                        continue
                    
                    self.logger.info(f"Importing dataset {year}-{month:02d}-{day:02d} hour {hour:02d}")
                    timestamp = datetime(year, month, day, hour)
                    
                    batch = []
                    rows_imported = 0
                    
                    # Update worker status at the start of processing
                    self.worker_monitor.update_worker_status(
                        worker_id, 
                        (year, month, day, hour),
                        0
                    )
                    
                    for record in self.processor.process_file(file_path, timestamp):
                        batch.append(record)
                        if len(batch) >= Config.BATCH_SIZE:
                            imported = self.db_manager.insert_batch(connection, batch)
                            rows_imported += imported
                            # Update status after each batch
                            self.worker_monitor.update_worker_status(
                                worker_id, 
                                (year, month, day, hour),
                                imported
                            )
                            batch = []
                    
                    if batch:
                        imported = self.db_manager.insert_batch(connection, batch)
                        rows_imported += imported
                        self.worker_monitor.update_worker_status(
                            worker_id, 
                            (year, month, day, hour),
                            imported
                        )
                    
                    if rows_imported > 0:
                        with self._lock:
                            self._processed_count += 1
                            self.successful_imports.add((year, month, day, hour))
                            self.state_manager.save_state(
                                self.failed_downloads,
                                self.successful_imports
                            )
                        
                        self.email_batch_manager.add_to_batch(
                            (year, month, day, hour),
                            rows_imported
                        )
                    
                except Exception as e:
                    self.logger.error(f"Error in import worker: {e}", exc_info=True)
                    with self._lock:
                        if dataset:
                            year, month, day, hour, _ = dataset
                            self.failed_downloads.add((year, month, day, hour))
                            self.state_manager.save_state(
                                self.failed_downloads,
                                self.successful_imports
                            )
                finally:
                    self.import_queue.task_done()
                    
        except Exception as e:
            self.logger.error(f"Critical error in import worker: {e}", exc_info=True)
        finally:
            if connection:
                connection.close()

    def _process_datasets(self, datasets):
        """Main processing loop with independent download and import processes"""
        try:
            # Reset events
            self.download_complete.clear()
            self.import_complete.clear()
            
            # Start download manager in a separate thread if we have download workers
            if Config.MAX_DOWNLOAD_WORKERS > 0:
                download_thread = threading.Thread(
                    target=self._download_manager,
                    args=(datasets,)
                )
                download_thread.start()
            else:
                # If no download workers, mark downloads as complete immediately
                self.download_complete.set()
            
            # Start import manager in a separate thread if we have import workers
            if Config.MAX_IMPORT_WORKERS > 0:
                import_thread = threading.Thread(
                    target=self._import_manager
                )
                import_thread.start()
            else:
                # If no import workers, mark imports as complete immediately
                self.import_complete.set()
            
            # Wait for both processes to complete
            self.download_complete.wait()
            self.import_complete.wait()
            
            # Check if any datasets need to be retried
            retry_datasets = list(self.failed_downloads)
            if retry_datasets:
                self.logger.info(f"Retrying {len(retry_datasets)} failed datasets after delay")
                time.sleep(Config.DOWNLOAD_RETRY_DELAY)
                self._process_datasets(retry_datasets)
            
        except Exception as e:
            self.logger.error(f"Error in processing loop: {e}", exc_info=True)
            self.notifier.send_notification(
                "WikiStat Import: Processing Error",
                f"Error occurred during processing: {str(e)}"
            )

    def run(self):
        try:
            # Initialize database
            connection = self.db_manager.get_connection()
            self.db_manager.create_schema(connection)
            connection.close()

            # Start the worker monitor BEFORE processing begins
            self.worker_monitor.start()
            
            try:
                # Initialize tasks based on active processes
                tasks = self._initialize_tasks()
                
                # Start processing with whatever tasks were initialized
                self._process_datasets(tasks)
            finally:
                # Ensure monitor is stopped even if processing fails
                self.worker_monitor.stop()
            
        except Exception as e:
            self.logger.error(f"Error in main execution: {e}", exc_info=True)
            self.notifier.send_notification(
                "WikiStat Import: Error",
                f"Error occurred during import process: {str(e)}"
            )

if __name__ == "__main__":
    logger = setup_logging()
    importer = WikistatImporter()
    importer.run()