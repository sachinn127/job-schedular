"""
Database models for Job Scheduler
"""

import uuid
from datetime import datetime
from flask_sqlalchemy import SQLAlchemy
from enum import Enum

db = SQLAlchemy()


class ExecutionTypeEnum(Enum):
    """Job execution semantics"""
    ATLEAST_ONCE = "ATLEAST_ONCE"
    AT_MOST_ONCE = "AT_MOST_ONCE"
    EXACTLY_ONCE = "EXACTLY_ONCE"


class JobStatusEnum(Enum):
    """Job status"""
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    DELETED = "DELETED"


class JobConfig(db.Model):
    """
    Stores job configuration
    Represents a single scheduled job definition
    """
    __tablename__ = 'job_configs'
    
    id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    schedule = db.Column(db.String(255), nullable=False)
    api_endpoint = db.Column(db.String(2048), nullable=False)
    execution_type = db.Column(db.String(50), nullable=False, default='ATLEAST_ONCE')
    status = db.Column(db.String(50), default='ACTIVE')
    job_metadata = db.Column(db.JSON, default={})
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_execution_time = db.Column(db.DateTime)
    
    executions = db.relationship('JobExecution', backref='job_config', lazy=True, cascade='all, delete-orphan')
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'jobId': self.id,
            'schedule': self.schedule,
            'api': self.api_endpoint,
            'type': self.execution_type,
            'status': self.status,
            'metadata': self.job_metadata,
            'createdAt': self.created_at.isoformat(),
            'updatedAt': self.updated_at.isoformat(),
            'lastExecutionTime': self.last_execution_time.isoformat() if self.last_execution_time else None
        }


class JobExecution(db.Model):
    """
    Stores job execution history
    Tracks each individual execution of a job
    """
    __tablename__ = 'job_executions'
    
    id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    job_config_id = db.Column(db.String(36), db.ForeignKey('job_configs.id'), nullable=False)
    
    execution_timestamp = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    scheduled_time = db.Column(db.DateTime, nullable=False)
    actual_start_time = db.Column(db.DateTime)
    actual_end_time = db.Column(db.DateTime)
    
    http_status_code = db.Column(db.Integer)
    execution_duration_ms = db.Column(db.Integer)
    
    status = db.Column(db.String(50))
    error_message = db.Column(db.Text)
    response_body = db.Column(db.Text)
    
    retry_count = db.Column(db.Integer, default=0)
    max_retries = db.Column(db.Integer, default=3)
    
    schedule_delay_ms = db.Column(db.Integer)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    
    __table_args__ = (
        db.Index('idx_job_config_execution_time', 'job_config_id', 'execution_timestamp'),
        db.Index('idx_job_config_status', 'job_config_id', 'status'),
    )
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'executionId': self.id,
            'jobId': self.job_config_id,
            'executionTimestamp': self.execution_timestamp.isoformat(),
            'scheduledTime': self.scheduled_time.isoformat(),
            'actualStartTime': self.actual_start_time.isoformat() if self.actual_start_time else None,
            'actualEndTime': self.actual_end_time.isoformat() if self.actual_end_time else None,
            'httpStatusCode': self.http_status_code,
            'executionDurationMs': self.execution_duration_ms,
            'status': self.status,
            'errorMessage': self.error_message,
            'retryCount': self.retry_count,
            'scheduleDelayMs': self.schedule_delay_ms
        }