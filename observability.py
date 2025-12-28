"""
Observability and Metrics Collection
Provides logging, metrics tracking, and monitoring capabilities
"""

import logging
import json
from datetime import datetime
from collections import defaultdict, deque
from threading import Lock
import os


def setup_logging(name):
    """Setup logging configuration"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Log format
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    
    # File handler
    log_dir = os.getenv('LOG_DIR', 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    file_handler = logging.FileHandler(f'{log_dir}/scheduler.log')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    
    # Add handlers
    if not logger.handlers:
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
    
    return logger


class MetricsCollector:
    """
    Collects and tracks metrics for the scheduler
    Thread-safe metrics collection
    """
    
    def __init__(self, max_history=1000):
        self.lock = Lock()
        self.max_history = max_history
        
        # Counters
        self.jobs_created = 0
        self.jobs_updated = 0
        self.jobs_deleted = 0
        
        self.job_creation_failures = 0
        self.job_update_failures = 0
        
        self.successful_executions = 0
        self.failed_executions = 0
        self.timeout_executions = 0
        
        # Latency tracking (in milliseconds)
        self.api_latencies = defaultdict(deque)
        self.execution_durations = deque(maxlen=max_history)
        
        # Execution history
        self.recent_executions = deque(maxlen=max_history)
        
        # Start time
        self.start_time = datetime.utcnow()
    
    # ==================== Counter Operations ====================
    
    def increment_jobs_created(self):
        """Increment job creation counter"""
        with self.lock:
            self.jobs_created += 1
    
    def increment_jobs_updated(self):
        """Increment job update counter"""
        with self.lock:
            self.jobs_updated += 1
    
    def increment_jobs_deleted(self):
        """Increment job deletion counter"""
        with self.lock:
            self.jobs_deleted += 1
    
    def increment_job_creation_failures(self):
        """Increment job creation failures"""
        with self.lock:
            self.job_creation_failures += 1
    
    def increment_job_update_failures(self):
        """Increment job update failures"""
        with self.lock:
            self.job_update_failures += 1
    
    def increment_successful_executions(self):
        """Increment successful execution counter"""
        with self.lock:
            self.successful_executions += 1
    
    def increment_failed_executions(self):
        """Increment failed execution counter"""
        with self.lock:
            self.failed_executions += 1
    
    def increment_timeout_executions(self):
        """Increment timeout execution counter"""
        with self.lock:
            self.timeout_executions += 1
    
    # ==================== Latency Tracking ====================
    
    def record_api_latency(self, endpoint: str, duration_ms: float):
        """Record API latency"""
        with self.lock:
            if len(self.api_latencies[endpoint]) >= self.max_history:
                self.api_latencies[endpoint].popleft()
            self.api_latencies[endpoint].append(duration_ms)
    
    def record_execution_duration(self, duration_ms: int):
        """Record job execution duration"""
        with self.lock:
            self.execution_durations.append(duration_ms)
    
    def record_execution(self, job_id: str, status: str, duration_ms: int):
        """Record execution event"""
        with self.lock:
            self.recent_executions.append({
                'jobId': job_id,
                'status': status,
                'durationMs': duration_ms,
                'timestamp': datetime.utcnow().isoformat()
            })
    
    # ==================== Metrics Retrieval ====================
    
    def get_metrics(self):
        """Get all metrics"""
        with self.lock:
            total_executions = self.successful_executions + self.failed_executions + self.timeout_executions
            success_rate = (
                (self.successful_executions / total_executions * 100)
                if total_executions > 0 else 0
            )
            
            avg_execution_duration = (
                sum(self.execution_durations) / len(self.execution_durations)
                if self.execution_durations else 0
            )
            
            uptime_seconds = (datetime.utcnow() - self.start_time).total_seconds()
            
            return {
                'timestamp': datetime.utcnow().isoformat(),
                'uptime_seconds': round(uptime_seconds, 2),
                'jobs': {
                    'created': self.jobs_created,
                    'updated': self.jobs_updated,
                    'deleted': self.jobs_deleted,
                    'creation_failures': self.job_creation_failures,
                    'update_failures': self.job_update_failures
                },
                'executions': {
                    'successful': self.successful_executions,
                    'failed': self.failed_executions,
                    'timeout': self.timeout_executions,
                    'total': total_executions,
                    'success_rate_percent': round(success_rate, 2),
                    'average_duration_ms': round(avg_execution_duration, 2)
                },
                'api_latency': self._get_api_latency_stats()
            }
    
    def _get_api_latency_stats(self) -> dict:
        """Get API latency statistics"""
        stats = {}
        
        for endpoint, latencies in self.api_latencies.items():
            if latencies:
                latencies_list = list(latencies)
                stats[endpoint] = {
                    'calls': len(latencies_list),
                    'average_ms': round(sum(latencies_list) / len(latencies_list), 2),
                    'min_ms': round(min(latencies_list), 2),
                    'max_ms': round(max(latencies_list), 2),
                    'p95_ms': round(sorted(latencies_list)[int(len(latencies_list) * 0.95)], 2)
                }
        
        return stats
    
    def get_execution_histogram(self, bucket_size_ms=100):
        """Get execution duration histogram"""
        histogram = defaultdict(int)
        
        with self.lock:
            for duration in self.execution_durations:
                bucket = (duration // bucket_size_ms) * bucket_size_ms
                histogram[bucket] += 1
        
        return dict(sorted(histogram.items()))
    
    def get_recent_executions(self, limit=50):
        """Get recent execution history"""
        with self.lock:
            return list(self.recent_executions)[-limit:]
    
    # ==================== Health Checks ====================
    
    def get_health_status(self):
        """Get health status"""
        with self.lock:
            total_executions = self.successful_executions + self.failed_executions
            
            if total_executions == 0:
                return {
                    'status': 'healthy',
                    'message': 'No executions yet'
                }
            
            failure_rate = self.failed_executions / total_executions
            
            if failure_rate > 0.5:
                return {
                    'status': 'critical',
                    'message': f'High failure rate: {failure_rate * 100:.1f}%',
                    'failure_rate': failure_rate
                }
            elif failure_rate > 0.1:
                return {
                    'status': 'degraded',
                    'message': f'Moderate failure rate: {failure_rate * 100:.1f}%',
                    'failure_rate': failure_rate
                }
            else:
                return {
                    'status': 'healthy',
                    'message': 'System operating normally',
                    'failure_rate': failure_rate
                }


class HealthChecker:
    """
    Performs health checks on the scheduler system
    """
    
    @staticmethod
    def check_database_connection(db):
        """Check if database is accessible"""
        try:
            db.session.execute('SELECT 1')
            return True, 'Database OK'
        except Exception as e:
            return False, f'Database error: {str(e)}'
    
    @staticmethod
    def check_scheduler_running(scheduler):
        """Check if scheduler is running"""
        if scheduler.running:
            return True, 'Scheduler running'
        else:
            return False, 'Scheduler not running'
    
    @staticmethod
    def get_system_health(db, scheduler, metrics):
        """Get overall system health"""
        db_ok, db_msg = HealthChecker.check_database_connection(db)
        sched_ok, sched_msg = HealthChecker.check_scheduler_running(scheduler)
        metrics_health = metrics.get_health_status()
        
        overall_healthy = db_ok and sched_ok and metrics_health['status'] == 'healthy'
        
        return {
            'overall_status': 'healthy' if overall_healthy else 'degraded',
            'database': {
                'ok': db_ok,
                'message': db_msg
            },
            'scheduler': {
                'ok': sched_ok,
                'message': sched_msg
            },
            'metrics': metrics_health,
            'timestamp': datetime.utcnow().isoformat()
        }