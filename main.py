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
        
        # Now set the total_datasets after initialization
        self.email_batch_manager.total_datasets = self._total_datasets

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
        """Main processing loop"""
        try:
            while datasets or self.failed_downloads:
                self.logger.info("Starting processing cycle")
                
                # Start download workers
                download_threads = []
                for i in range(Config.MAX_DOWNLOAD_WORKERS):
                    t = threading.Thread(target=self._download_worker)
                    t.daemon = True
                    t.start()
                    download_threads.append(t)
                
                # Start import workers
                import_threads = []
                for i in range(Config.MAX_IMPORT_WORKERS):
                    t = threading.Thread(target=self._import_worker)
                    t.daemon = True
                    t.start()
                    import_threads.append(t)
                
                self.logger.info(f"Adding {len(datasets)} datasets to download queue")
                # Add datasets to download queue
                for dataset in datasets:
                    self.download_queue.put(dataset)
                
                # Add poison pills for download workers
                for _ in range(Config.MAX_DOWNLOAD_WORKERS):
                    self.download_queue.put((None, None, None))
                
                # Wait for downloads to complete
                self.download_queue.join()
                self.logger.info("All downloads completed")
                
                # Add poison pills for import workers
                for _ in range(Config.MAX_IMPORT_WORKERS):
                    self.import_queue.put(None)
                
                # Wait for imports to complete
                self.import_queue.join()
                self.logger.info("All imports completed")
                
                # Wait for all threads to finish
                for t in download_threads + import_threads:
                    t.join()
                
                # Update datasets list with any remaining failed downloads
                datasets = list(self.failed_downloads)
                self.failed_downloads.clear()
                
                if datasets:
                    self.logger.info(f"Retrying {len(datasets)} failed datasets after a short delay")
                    time.sleep(300)  # 5 minute delay before retrying
            
            # Send completion notification
            self.notifier.send_notification(
                "WikiStat Import: All Datasets Completed",
                f"""
                Successfully processed {self._processed_count} datasets.
                Failed downloads remaining: {len(self.failed_downloads)}
                """
            )
            
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

            # Generate download tasks
            tasks = self.downloader.generate_download_tasks()
            self._total_datasets = len(tasks)
            self.email_batch_manager.total_datasets = self._total_datasets
            self.logger.info(f"Found {self._total_datasets} datasets to process")
            
            # Start the worker monitor BEFORE processing begins
            self.worker_monitor.start()
            
            try:
                # Add all tasks to the download queue
                for task in tasks:
                    if task not in self.successful_imports:
                        self.download_queue.put(task)
                
                # Start processing
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