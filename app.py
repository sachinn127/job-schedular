"""
High-Throughput Job Scheduler
Main application entry point with Flask API server
"""

import os
import json
import logging
from datetime import datetime
from flask import Flask, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
import requests
from functools import wraps
import time

from models import JobConfig, JobExecution, db
from scheduler_service import SchedulerService
from job_executor import JobExecutor
from observability import MetricsCollector, setup_logging

# Initialize Flask app
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL', 'sqlite:///scheduler.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Initialize database
db.init_app(app)

# Initialize services
scheduler_service = None
metrics_collector = MetricsCollector()
job_executor = JobExecutor()

# Setup logging
logger = setup_logging(__name__)


def measure_time(f):
    """Decorator to measure API response time"""
    @wraps(f)
    def decorated(*args, **kwargs):
        start = time.time()
        result = f(*args, **kwargs)
        duration = time.time() - start
        metrics_collector.record_api_latency(f.__name__, duration)
        return result
    return decorated


# ==================== API Endpoints ====================

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat()
    }), 200


@app.route('/jobs', methods=['POST'])
@measure_time
def create_job():
    """
    Create a new job
    Request body: Job Specification (JSON)
    Returns: jobId
    """
    try:
        data = request.get_json()
        
        # Validate job spec
        required_fields = ['schedule', 'api', 'type']
        if not all(field in data for field in required_fields):
            return jsonify({
                'error': 'Missing required fields: schedule, api, type'
            }), 400
        
        # Create job config
        job_config = JobConfig(
            schedule=data['schedule'],
            api_endpoint=data['api'],
            execution_type=data['type'],
            metadata=data.get('metadata', {})
        )
        
        # Create job in service
        job_id = scheduler_service.create_job(job_config)
        
        logger.info(f"Job created: {job_id} with schedule {data['schedule']}")
        metrics_collector.increment_jobs_created()
        
        return jsonify({
            'jobId': job_id,
            'message': 'Job created successfully'
        }), 201
        
    except Exception as e:
        logger.error(f"Error creating job: {str(e)}")
        metrics_collector.increment_job_creation_failures()
        return jsonify({'error': str(e)}), 400


@app.route('/jobs/<job_id>', methods=['GET'])
@measure_time
def get_job(job_id):
    """Get job details"""
    try:
        job = scheduler_service.get_job(job_id)
        if not job:
            return jsonify({'error': 'Job not found'}), 404
        
        return jsonify(job.to_dict()), 200
    except Exception as e:
        logger.error(f"Error fetching job: {str(e)}")
        return jsonify({'error': str(e)}), 400


@app.route('/jobs/<job_id>', methods=['PUT'])
@measure_time
def update_job(job_id):
    """
    Modify an existing job
    Only allows updates to schedule, api, and metadata
    """
    try:
        data = request.get_json()
        job = scheduler_service.update_job(job_id, data)
        
        if not job:
            return jsonify({'error': 'Job not found'}), 404
        
        logger.info(f"Job updated: {job_id}")
        metrics_collector.increment_jobs_updated()
        
        return jsonify({
            'jobId': job_id,
            'message': 'Job updated successfully',
            'job': job.to_dict()
        }), 200
        
    except Exception as e:
        logger.error(f"Error updating job: {str(e)}")
        metrics_collector.increment_job_update_failures()
        return jsonify({'error': str(e)}), 400


@app.route('/jobs/<job_id>/executions', methods=['GET'])
@measure_time
def get_job_executions(job_id):
    """
    Get the last 5 executions for a given jobId
    Returns execution records with timestamp, status, and duration
    """
    try:
        limit = request.args.get('limit', default=5, type=int)
        limit = min(limit, 100)  # Max limit to prevent abuse
        
        executions = scheduler_service.get_job_executions(job_id, limit)
        
        if not executions and not scheduler_service.get_job(job_id):
            return jsonify({'error': 'Job not found'}), 404
        
        execution_list = [
            {
                'executionId': e.id,
                'timestamp': e.execution_timestamp.isoformat(),
                'status': e.http_status_code,
                'duration': e.execution_duration_ms,
                'success': 200 <= e.http_status_code < 300 if e.http_status_code else False,
                'errorMessage': e.error_message
            }
            for e in executions
        ]
        
        return jsonify({
            'jobId': job_id,
            'executionCount': len(execution_list),
            'executions': execution_list
        }), 200
        
    except Exception as e:
        logger.error(f"Error fetching executions: {str(e)}")
        return jsonify({'error': str(e)}), 400


@app.route('/jobs', methods=['GET'])
@measure_time
def list_jobs():
    """List all jobs with pagination"""
    try:
        page = request.args.get('page', default=1, type=int)
        per_page = request.args.get('per_page', default=20, type=int)
        
        jobs, total = scheduler_service.list_jobs(page, per_page)
        
        return jsonify({
            'page': page,
            'per_page': per_page,
            'total': total,
            'jobs': [job.to_dict() for job in jobs]
        }), 200
        
    except Exception as e:
        logger.error(f"Error listing jobs: {str(e)}")
        return jsonify({'error': str(e)}), 400


@app.route('/jobs/<job_id>/pause', methods=['POST'])
@measure_time
def pause_job(job_id):
    """Pause a job"""
    try:
        success = scheduler_service.pause_job(job_id)
        if not success:
            return jsonify({'error': 'Job not found'}), 404
        
        logger.info(f"Job paused: {job_id}")
        return jsonify({'message': 'Job paused successfully'}), 200
        
    except Exception as e:
        logger.error(f"Error pausing job: {str(e)}")
        return jsonify({'error': str(e)}), 400


@app.route('/jobs/<job_id>/resume', methods=['POST'])
@measure_time
def resume_job(job_id):
    """Resume a paused job"""
    try:
        success = scheduler_service.resume_job(job_id)
        if not success:
            return jsonify({'error': 'Job not found'}), 404
        
        logger.info(f"Job resumed: {job_id}")
        return jsonify({'message': 'Job resumed successfully'}), 200
        
    except Exception as e:
        logger.error(f"Error resuming job: {str(e)}")
        return jsonify({'error': str(e)}), 400


@app.route('/jobs/<job_id>', methods=['DELETE'])
@measure_time
def delete_job(job_id):
    """Delete a job"""
    try:
        success = scheduler_service.delete_job(job_id)
        if not success:
            return jsonify({'error': 'Job not found'}), 404
        
        logger.info(f"Job deleted: {job_id}")
        return jsonify({'message': 'Job deleted successfully'}), 200
        
    except Exception as e:
        logger.error(f"Error deleting job: {str(e)}")
        return jsonify({'error': str(e)}), 400


# ==================== Observability Endpoints ====================

@app.route('/metrics', methods=['GET'])
def get_metrics():
    """Get system metrics"""
    return jsonify(metrics_collector.get_metrics()), 200


@app.route('/scheduler/status', methods=['GET'])
def scheduler_status():
    """Get scheduler status"""
    try:
        status = scheduler_service.get_scheduler_status()
        return jsonify(status), 200
    except Exception as e:
        logger.error(f"Error getting scheduler status: {str(e)}")
        return jsonify({'error': str(e)}), 400


@app.route('/jobs/<job_id>/stats', methods=['GET'])
def get_job_stats(job_id):
    """Get job execution statistics"""
    try:
        stats = scheduler_service.get_job_stats(job_id)
        if not stats:
            return jsonify({'error': 'Job not found'}), 404
        return jsonify(stats), 200
    except Exception as e:
        logger.error(f"Error getting job stats: {str(e)}")
        return jsonify({'error': str(e)}), 400


# ==================== Error Handlers ====================

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404


@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal server error: {str(error)}")
    return jsonify({'error': 'Internal server error'}), 500


# ==================== Application Initialization ====================

def initialize_scheduler():
    """Initialize the scheduler service"""
    global scheduler_service
    scheduler_service = SchedulerService(db, metrics_collector)
    scheduler_service.start()
    logger.info("Scheduler service initialized and started")


@app.before_request
def before_request():
    """Ensure scheduler is initialized"""
    if scheduler_service is None:
        initialize_scheduler()


@app.teardown_appcontext
def shutdown_session(exception=None):
    """Clean up on shutdown"""
    if scheduler_service:
        scheduler_service.shutdown()


if __name__ == '__main__':
    with app.app_context():
        db.create_all()
        initialize_scheduler()
    
    app.run(
        host='0.0.0.0',
        port=int(os.getenv('PORT', 5000)),
        debug=os.getenv('FLASK_ENV') == 'development'
    )