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
        self.total_processed_datasets = 0
        self.total_rows_imported = 0
        self.hourly_processed_datasets = 0
        self.hourly_rows_imported = 0
        self.last_hourly_reset = datetime.now()
        
    def add_to_batch(self, dataset, rows_imported):
        """Add a processed dataset to the current batch without sending individual emails"""
        with self.lock:
            timestamp = dataset[0:3]  # (year, month, day)
            self.current_batch[timestamp] += rows_imported
            self.total_processed_datasets += 1
            self.total_rows_imported += rows_imported
            self.hourly_processed_datasets += 1
            self.hourly_rows_imported += rows_imported
            
            current_time = datetime.now()
            if (current_time - self.last_email_time).total_seconds() >= self.batch_interval:
                self._send_hourly_report()
    
    def _estimate_completion_time(self, remaining_datasets):
        """Estimate completion time based on hourly processing rate"""
        current_time = datetime.now()
        elapsed_time = (current_time - self.last_hourly_reset).total_seconds()
        
        if elapsed_time == 0 or self.hourly_processed_datasets == 0:
            return "Unable to estimate completion time yet"
            
        # Calculate hourly rate based on recent performance
        datasets_per_hour = (self.hourly_processed_datasets / elapsed_time) * 3600
        if datasets_per_hour == 0:
            return "Unable to estimate completion time at current rate"
            
        remaining_hours = remaining_datasets / datasets_per_hour
        remaining_time = timedelta(hours=remaining_hours)
        estimated_completion = current_time + remaining_time
        
        days = int(remaining_time.total_seconds() / 86400)
        hours = int((remaining_time.total_seconds() % 86400) / 3600)
        
        return (
            f"Estimated completion time: {estimated_completion.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"(approximately {days} days, {hours} hours remaining)\n"
            f"Current processing rate: {int(datasets_per_hour)} datasets per hour"
        )
    
    def _send_hourly_report(self):
        """Send hourly status report and reset hourly counters"""
        current_time = datetime.now()
        
        # Calculate remaining datasets
        from state_manager import StateManager
        state_mgr = StateManager()
        _, successful_imports = state_mgr.load_state()
        total_datasets = getattr(self, 'total_datasets', 0)  # Safely get total_datasets
        remaining_datasets = total_datasets - len(successful_imports)
        
        message = (
            f"Hourly Import Summary\n"
            f"Time period: {self.last_email_time.strftime('%Y-%m-%d %H:%M')} to "
            f"{current_time.strftime('%Y-%m-%d %H:%M')}\n\n"
            f"Datasets processed in this period: {self.hourly_processed_datasets:,}\n"
            f"Total rows imported in this period: {self.hourly_rows_imported:,}\n"
            f"Total datasets completed: {len(successful_imports):,}/{total_datasets:,}\n"
            f"Datasets remaining: {remaining_datasets:,}\n\n"
            f"{self._estimate_completion_time(remaining_datasets)}\n\n"
            f"Total rows imported since start: {self.total_rows_imported:,}"
        )
        
        self.notifier.send_notification(
            "WikiStat Import: Hourly Update",
            message
        )
        
        # Reset hourly counters but keep total counters
        self.hourly_processed_datasets = 0
        self.hourly_rows_imported = 0
        self.last_email_time = current_time
        self.last_hourly_reset = current_time
        self.current_batch.clear()
