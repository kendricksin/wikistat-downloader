# email_batch_manager.py
from datetime import datetime, timedelta
import threading
from collections import defaultdict
import logging

class EmailBatchManager:
    def __init__(self, notifier, batch_interval=3600):  # 3600 seconds = 1 hour
        self.notifier = notifier
        self.batch_interval = batch_interval
        self.current_batch = defaultdict(int)
        self.last_email_time = datetime.now()
        self.lock = threading.Lock()
        self.logger = logging.getLogger(__name__)
        self._start_time = datetime.now()
        self.processed_since_start = 0
        
    def add_to_batch(self, dataset, rows_imported):
        """Add a processed dataset to the current batch"""
        with self.lock:
            timestamp = dataset[0:3]  # (year, month, day)
            self.current_batch[timestamp] += rows_imported
            self.processed_since_start += rows_imported
            self._check_and_send()
    
    def _estimate_completion_time(self, remaining_datasets):
        """Estimate completion time based on current processing rate"""
        elapsed_time = (datetime.now() - self._start_time).total_seconds()
        if elapsed_time == 0 or self.processed_since_start == 0:
            return "Unable to estimate completion time yet"
            
        datasets_per_second = self.processed_since_start / elapsed_time
        if datasets_per_second == 0:
            return "Unable to estimate completion time at current rate"
            
        remaining_time_seconds = remaining_datasets / datasets_per_second
        remaining_time = timedelta(seconds=int(remaining_time_seconds))
        estimated_completion = datetime.now() + remaining_time
        
        return (
            f"Estimated completion time: {estimated_completion.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"(approximately {remaining_time.days} days, "
            f"{remaining_time.seconds // 3600} hours remaining)"
        )
    
    def _check_and_send(self):
        """Check if it's time to send a batch email and send if necessary"""
        current_time = datetime.now()
        if (current_time - self.last_email_time).total_seconds() >= self.batch_interval:
            if self.current_batch:
                total_rows = sum(self.current_batch.values())
                datasets_processed = len(self.current_batch)
                
                # Calculate remaining datasets based on state manager data
                from state_manager import StateManager
                state_mgr = StateManager()
                _, successful_imports = state_mgr.load_state()
                total_datasets = self.total_datasets
                remaining_datasets = total_datasets - len(successful_imports)
                
                message = (
                    f"Hourly Import Summary\n"
                    f"Time period: {self.last_email_time.strftime('%Y-%m-%d %H:%M')} to "
                    f"{current_time.strftime('%Y-%m-%d %H:%M')}\n\n"
                    f"Datasets processed in this period: {datasets_processed}\n"
                    f"Total rows imported in this period: {total_rows:,}\n"
                    f"Total datasets completed: {len(successful_imports):,}/{total_datasets:,}\n"
                    f"Datasets remaining: {remaining_datasets:,}\n\n"
                    f"{self._estimate_completion_time(remaining_datasets)}"
                )
                
                self.notifier.send_notification(
                    "WikiStat Import: Hourly Update",
                    message
                )
                
                self.current_batch.clear()
                self.last_email_time = current_time