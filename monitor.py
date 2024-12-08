# monitor.py
import threading
import time
import logging
from datetime import datetime

class WorkerMonitor:
    def __init__(self, interval=60):  # Changed from 600 to 60 seconds
        self.interval = interval
        self.logger = logging.getLogger(__name__)
        self.workers_status = {}
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._monitor_thread = None
        self.total_rows_imported = 0
        self.start_time = None
    
    def update_worker_status(self, worker_id, dataset, rows_imported):
        """Update status for a specific worker with cumulative counting"""
        with self._lock:
            if not self.start_time:
                self.start_time = datetime.now()
                
            if worker_id not in self.workers_status:
                self.workers_status[worker_id] = {
                    'dataset': dataset,
                    'rows_imported': 0,
                    'last_update': datetime.now(),
                    'state': 'active' if dataset else 'idle'
                }
            
            current_status = self.workers_status[worker_id]
            
            # If worker starts processing a new dataset, log the transition
            if dataset != current_status['dataset']:
                if dataset:
                    self.logger.info(f"Worker {worker_id} starting dataset: {dataset}")
                    current_status['state'] = 'active'
                else:
                    self.logger.info(f"Worker {worker_id} completed dataset: {current_status['dataset']}")
                    current_status['state'] = 'idle'
                current_status['rows_imported'] = 0
                
            # Update the worker's status
            current_status['dataset'] = dataset
            current_status['rows_imported'] += rows_imported
            current_status['last_update'] = datetime.now()
            
            # Update total rows imported
            self.total_rows_imported += rows_imported
    
    def _log_status(self):
        """Log current status of all workers"""
        with self._lock:
            runtime = datetime.now() - self.start_time if self.start_time else datetime.now() - datetime.now()
            
            status_lines = [
                "=== Worker Status Report ===",
                f"Runtime: {runtime}",
                f"Total rows imported: {self.total_rows_imported:,}"
            ]
            
            active_workers = 0
            idle_workers = 0
            
            for worker_id, status in self.workers_status.items():
                if status['state'] == 'active':
                    active_workers += 1
                    if status['dataset']:
                        year, month, day, hour = status['dataset']
                        status_lines.append(
                            f"Worker {worker_id}: Processing {year}-{month:02d}-{day:02d} hour {hour:02d}, "
                            f"Rows imported: {status['rows_imported']:,}, "
                            f"Last update: {status['last_update'].strftime('%Y-%m-%d %H:%M:%S')}"
                        )
                else:
                    idle_workers += 1
            
            status_lines.append(f"Active workers: {active_workers}")
            status_lines.append(f"Idle workers: {idle_workers}")
            status_lines.append("=" * 25)
            
            self.logger.info("\n".join(status_lines))
    
    def _monitor_loop(self):
        """Main monitoring loop"""
        while not self._stop_event.is_set():
            self._log_status()
            self._stop_event.wait(self.interval)
    
    def start(self):
        """Start the monitoring thread"""
        self.start_time = datetime.now()
        self._monitor_thread = threading.Thread(target=self._monitor_loop)
        self._monitor_thread.daemon = True
        self._monitor_thread.start()
        self.logger.info("Worker monitor started")
    
    def stop(self):
        """Stop the monitoring thread"""
        self._stop_event.set()
        if self._monitor_thread:
            self._monitor_thread.join()
        self._log_status()  # Log final status before stopping
        self.logger.info("Worker monitor stopped")

    def reset_worker(self, worker_id):
        """Reset a worker's statistics when it starts a new dataset"""
        with self._lock:
            if worker_id in self.workers_status:
                self.workers_status[worker_id]['rows_imported'] = 0
                self.workers_status[worker_id]['state'] = 'idle'