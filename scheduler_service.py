"""
Core Scheduler Service
Manages job lifecycle and scheduling
"""

import logging
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

from models import JobConfig, JobExecution, db, JobStatusEnum
from job_executor import JobExecutor
from cron_parser import CronParser

logger = logging.getLogger(__name__)


class SchedulerService:
    """
    Main scheduler service that manages all jobs
    Responsibilities:
    - Create/update/delete jobs
    - Schedule jobs with APScheduler
    - Manage execution history
    - Track metrics and statistics
    """
    
    def __init__(self, database, metrics_collector):
        self.db = database
        self.metrics = metrics_collector
        self.scheduler = BackgroundScheduler(daemon=True)
        self.job_executor = JobExecutor()
        self.cron_parser = CronParser()
        self.running = False
    
    def start(self):
        """Start the scheduler"""
        try:
            self.scheduler.start()
            self.running = True
            logger.info("Scheduler started successfully")
            
            # Load and reschedule any existing jobs from database
            self._restore_jobs()
            
        except Exception as e:
            logger.error(f"Error starting scheduler: {str(e)}")
            raise
    
    def shutdown(self):
        """Shutdown the scheduler gracefully"""
        try:
            if self.scheduler.running:
                self.scheduler.shutdown(wait=True)
                self.running = False
                logger.info("Scheduler shut down successfully")
        except Exception as e:
            logger.error(f"Error during scheduler shutdown: {str(e)}")
    
    def create_job(self, job_config):
        """
        Create a new job and schedule it
        Returns: job_id
        """
        try:
            # Save job config to database
            self.db.session.add(job_config)
            self.db.session.commit()
            
            job_id = job_config.id
            
            # Schedule the job
            self._schedule_job(job_config)
            
            logger.info(f"Job created and scheduled: {job_id}")
            return job_id
            
        except Exception as e:
            self.db.session.rollback()
            logger.error(f"Error creating job: {str(e)}")
            raise
    
    def get_job(self, job_id):
        """Retrieve job configuration"""
        return JobConfig.query.filter_by(id=job_id).first()
    
    def update_job(self, job_id, updates):
        """
        Update job configuration
        Can update: schedule, api_endpoint, metadata
        """
        try:
            job = self.get_job(job_id)
            if not job:
                return None
            
            if 'schedule' in updates:
                job.schedule = updates['schedule']
                # Reschedule with new schedule
                self._reschedule_job(job)
            
            if 'api' in updates:
                job.api_endpoint = updates['api']
            
            if 'metadata' in updates:
                job.metadata = updates.get('metadata', {})
            
            job.updated_at = datetime.utcnow()
            self.db.session.commit()
            
            logger.info(f"Job updated: {job_id}")
            return job
            
        except Exception as e:
            self.db.session.rollback()
            logger.error(f"Error updating job: {str(e)}")
            raise
    
    def delete_job(self, job_id):
        """Delete a job and unschedule it"""
        try:
            job = self.get_job(job_id)
            if not job:
                return False
            
            # Remove from scheduler
            self.scheduler.remove_job(job_id)
            
            # Delete from database
            self.db.session.delete(job)
            self.db.session.commit()
            
            logger.info(f"Job deleted: {job_id}")
            return True
            
        except Exception as e:
            self.db.session.rollback()
            logger.error(f"Error deleting job: {str(e)}")
            raise
    
    def pause_job(self, job_id):
        """Pause a job execution"""
        try:
            job = self.get_job(job_id)
            if not job:
                return False
            
            job.status = JobStatusEnum.PAUSED.value
            self.db.session.commit()
            
            # Pause in scheduler
            if self.scheduler.get_job(job_id):
                self.scheduler.pause_job(job_id)
            
            logger.info(f"Job paused: {job_id}")
            return True
            
        except Exception as e:
            self.db.session.rollback()
            logger.error(f"Error pausing job: {str(e)}")
            raise
    
    def resume_job(self, job_id):
        """Resume a paused job"""
        try:
            job = self.get_job(job_id)
            if not job:
                return False
            
            job.status = JobStatusEnum.ACTIVE.value
            self.db.session.commit()
            
            # Resume in scheduler
            if self.scheduler.get_job(job_id):
                self.scheduler.resume_job(job_id)
            
            logger.info(f"Job resumed: {job_id}")
            return True
            
        except Exception as e:
            self.db.session.rollback()
            logger.error(f"Error resuming job: {str(e)}")
            raise
    
    def get_job_executions(self, job_id, limit=5):
        """Get last N executions for a job"""
        return JobExecution.query.filter_by(
            job_config_id=job_id
        ).order_by(
            JobExecution.execution_timestamp.desc()
        ).limit(limit).all()
    
    def list_jobs(self, page=1, per_page=20):
        """List all jobs with pagination"""
        query = JobConfig.query.order_by(JobConfig.created_at.desc())
        total = query.count()
        jobs = query.paginate(page=page, per_page=per_page).items
        return jobs, total
    
    def get_job_stats(self, job_id):
        """Get execution statistics for a job"""
        job = self.get_job(job_id)
        if not job:
            return None
        
        executions = JobExecution.query.filter_by(job_config_id=job_id).all()
        
        if not executions:
            return {
                'jobId': job_id,
                'totalExecutions': 0,
                'successCount': 0,
                'failureCount': 0,
                'successRate': 0,
                'averageDurationMs': 0,
                'maxScheduleDelayMs': 0,
                'averageScheduleDelayMs': 0
            }
        
        successful = [e for e in executions if e.status == 'SUCCESS']
        failed = [e for e in executions if e.status == 'FAILURE']
        
        avg_duration = sum(e.execution_duration_ms or 0 for e in executions) / len(executions)
        avg_delay = sum(e.schedule_delay_ms or 0 for e in executions) / len(executions)
        max_delay = max((e.schedule_delay_ms or 0) for e in executions)
        
        return {
            'jobId': job_id,
            'schedule': job.schedule,
            'totalExecutions': len(executions),
            'successCount': len(successful),
            'failureCount': len(failed),
            'successRate': len(successful) / len(executions) * 100,
            'averageDurationMs': round(avg_duration, 2),
            'maxScheduleDelayMs': max_delay,
            'averageScheduleDelayMs': round(avg_delay, 2),
            'lastExecution': job.last_execution_time.isoformat() if job.last_execution_time else None
        }
    
    def get_scheduler_status(self):
        """Get overall scheduler status"""
        jobs_count = JobConfig.query.filter_by(status='ACTIVE').count()
        paused_count = JobConfig.query.filter_by(status='PAUSED').count()
        
        recent_executions = JobExecution.query.filter(
            JobExecution.created_at >= datetime.utcnow() - timedelta(hours=1)
        ).count()
        
        recent_failures = JobExecution.query.filter(
            JobExecution.created_at >= datetime.utcnow() - timedelta(hours=1),
            JobExecution.status == 'FAILURE'
        ).count()
        
        return {
            'running': self.running,
            'activeJobs': jobs_count,
            'pausedJobs': paused_count,
            'recentExecutions': recent_executions,
            'recentFailures': recent_failures,
            'uptime': None  # Can be extended to track uptime
        }
    
    # ==================== Private Methods ====================
    
    def _schedule_job(self, job_config):
        """Schedule a job with APScheduler"""
        try:
            trigger = self._create_trigger(job_config.schedule)
            
            self.scheduler.add_job(
                self._execute_job,
                trigger=trigger,
                id=job_config.id,
                args=[job_config.id],
                name=f"Job: {job_config.id}",
                coalesce=True,  # Skip missed runs if delayed
                max_instances=1,  # Only one instance running at a time
                replace_existing=True
            )
            
            logger.info(f"Job scheduled: {job_config.id} with schedule {job_config.schedule}")
            
        except Exception as e:
            logger.error(f"Error scheduling job {job_config.id}: {str(e)}")
            raise
    
    def _reschedule_job(self, job_config):
        """Reschedule an existing job with new schedule"""
        try:
            # Remove old job
            if self.scheduler.get_job(job_config.id):
                self.scheduler.remove_job(job_config.id)
            
            # Schedule with new trigger
            self._schedule_job(job_config)
            
        except Exception as e:
            logger.error(f"Error rescheduling job {job_config.id}: {str(e)}")
            raise
    
    def _restore_jobs(self):
        """Restore and reschedule jobs from database on startup"""
        try:
            active_jobs = JobConfig.query.filter_by(status='ACTIVE').all()
            
            for job in active_jobs:
                try:
                    self._schedule_job(job)
                except Exception as e:
                    logger.error(f"Failed to restore job {job.id}: {str(e)}")
            
            logger.info(f"Restored {len(active_jobs)} jobs from database")
            
        except Exception as e:
            logger.error(f"Error restoring jobs: {str(e)}")
    
    def _create_trigger(self, cron_expression):
        """Create APScheduler trigger from CRON expression with seconds"""
        try:
            # Parse custom CRON format: "seconds minutes hours day month dayofweek"
            parts = cron_expression.strip().split()
            
            if len(parts) != 6:
                raise ValueError("CRON expression must have 6 fields: second minute hour day month dayofweek")
            
            second, minute, hour, day, month, day_of_week = parts
            
            # Create CronTrigger for APScheduler
            trigger = CronTrigger(
                second=second,
                minute=minute,
                hour=hour,
                day=day,
                month=month,
                day_of_week=day_of_week,
                timezone=pytz.UTC
            )
            
            return trigger
            
        except Exception as e:
            logger.error(f"Error creating trigger for {cron_expression}: {str(e)}")
            raise
    
    def _execute_job(self, job_id):
        """Execute a scheduled job"""
        try:
            job_config = self.get_job(job_id)
            if not job_config or job_config.status != 'ACTIVE':
                return
            
            # Execute the job
            execution_result = self.job_executor.execute(
                job_id=job_id,
                api_endpoint=job_config.api_endpoint,
                execution_type=job_config.execution_type
            )
            
            # Create execution record
            execution = JobExecution(
                job_config_id=job_id,
                execution_timestamp=datetime.utcnow(),
                scheduled_time=datetime.utcnow(),  # In real system, would track actual scheduled time
                actual_start_time=execution_result.get('start_time'),
                actual_end_time=execution_result.get('end_time'),
                http_status_code=execution_result.get('status_code'),
                execution_duration_ms=execution_result.get('duration_ms'),
                status=execution_result.get('status'),
                error_message=execution_result.get('error_message'),
                response_body=execution_result.get('response_body'),
                schedule_delay_ms=execution_result.get('schedule_delay_ms')
            )
            
            self.db.session.add(execution)
            job_config.last_execution_time = datetime.utcnow()
            self.db.session.commit()
            
            # Record metrics
            if execution.status == 'SUCCESS':
                self.metrics.increment_successful_executions()
            else:
                self.metrics.increment_failed_executions()
            
            self.metrics.record_execution_duration(execution.execution_duration_ms)
            
            logger.info(f"Job executed: {job_id}, Status: {execution.status}")
            
        except Exception as e:
            logger.error(f"Error executing job {job_id}: {str(e)}")
            self.metrics.increment_failed_executions()