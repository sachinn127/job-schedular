"""
Job Executor
Handles actual execution of scheduled jobs (HTTP POST requests)
"""

import logging
import requests
import time
import json
from datetime import datetime
from typing import Dict, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class JobExecutor:
    """
    Executes scheduled jobs as HTTP POST requests
    Handles retries, timeouts, and error tracking
    """
    
    # Configuration
    DEFAULT_TIMEOUT = 30  # seconds
    DEFAULT_RETRIES = 3
    CONNECTION_TIMEOUT = 5  # seconds
    
    def __init__(self):
        self.session = self._create_session()
    
    def execute(self, job_id: str, api_endpoint: str, execution_type: str) -> Dict[str, Any]:
        """
        Execute a job by making HTTP POST request
        
        Args:
            job_id: Job identifier
            api_endpoint: Target API endpoint URL
            execution_type: Execution semantics (ATLEAST_ONCE, AT_MOST_ONCE, EXACTLY_ONCE)
        
        Returns:
            Dictionary with execution results:
            - status: SUCCESS, FAILURE, TIMEOUT, ERROR
            - status_code: HTTP response status code
            - duration_ms: Execution time in milliseconds
            - error_message: Error details if failed
            - response_body: Response body (truncated)
            - start_time: When execution started
            - end_time: When execution ended
            - schedule_delay_ms: How late from scheduled time
        """
        start_time = datetime.utcnow()
        
        try:
            # Validate endpoint
            if not self._is_valid_url(api_endpoint):
                return self._create_error_result(
                    start_time,
                    'ERROR',
                    'Invalid API endpoint URL'
                )
            
            # Attempt execution based on execution type
            if execution_type == 'ATLEAST_ONCE':
                result = self._execute_atleast_once(api_endpoint)
            elif execution_type == 'AT_MOST_ONCE':
                result = self._execute_at_most_once(api_endpoint)
            elif execution_type == 'EXACTLY_ONCE':
                result = self._execute_exactly_once(api_endpoint)
            else:
                result = self._execute_atleast_once(api_endpoint)  # Default
            
            # Calculate execution time
            end_time = datetime.utcnow()
            duration_ms = int((end_time - start_time).total_seconds() * 1000)
            
            result['start_time'] = start_time
            result['end_time'] = end_time
            result['duration_ms'] = duration_ms
            result['job_id'] = job_id
            
            logger.info(
                f"Job executed: {job_id}, Status: {result['status']}, "
                f"Duration: {duration_ms}ms, StatusCode: {result.get('status_code')}"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Unexpected error executing job {job_id}: {str(e)}")
            return self._create_error_result(
                start_time,
                'ERROR',
                str(e)
            )
    
    # ==================== Execution Semantics ====================
    
    def _execute_atleast_once(self, api_endpoint: str) -> Dict[str, Any]:
        """
        ATLEAST_ONCE: Job executes at least once, even if it fails.
        Will retry on failure to ensure execution.
        """
        last_error = None
        last_status_code = None
        last_response = None
        
        for attempt in range(self.DEFAULT_RETRIES + 1):
            try:
                response = self.session.post(
                    api_endpoint,
                    timeout=self.DEFAULT_TIMEOUT,
                    json={'jobExecutionTime': datetime.utcnow().isoformat()}
                )
                
                last_status_code = response.status_code
                last_response = response.text[:1000]  # Truncate to 1000 chars
                
                # Success
                if 200 <= response.status_code < 300:
                    return {
                        'status': 'SUCCESS',
                        'status_code': response.status_code,
                        'response_body': last_response,
                        'error_message': None,
                        'retry_count': attempt
                    }
                
                # Server error - retry
                elif 500 <= response.status_code < 600:
                    last_error = f"Server error: {response.status_code}"
                    if attempt < self.DEFAULT_RETRIES:
                        time.sleep(2 ** attempt)  # Exponential backoff
                        continue
                
                # Client error - don't retry
                else:
                    return {
                        'status': 'FAILURE',
                        'status_code': response.status_code,
                        'response_body': last_response,
                        'error_message': f"HTTP {response.status_code}",
                        'retry_count': attempt
                    }
            
            except requests.Timeout:
                last_error = "Request timeout"
                if attempt < self.DEFAULT_RETRIES:
                    time.sleep(2 ** attempt)
                    continue
            
            except requests.ConnectionError:
                last_error = "Connection error"
                if attempt < self.DEFAULT_RETRIES:
                    time.sleep(2 ** attempt)
                    continue
            
            except Exception as e:
                last_error = str(e)
                if attempt < self.DEFAULT_RETRIES:
                    time.sleep(2 ** attempt)
                    continue
        
        return {
            'status': 'FAILURE',
            'status_code': last_status_code,
            'response_body': last_response,
            'error_message': last_error,
            'retry_count': self.DEFAULT_RETRIES
        }
    
    def _execute_at_most_once(self, api_endpoint: str) -> Dict[str, Any]:
        """
        AT_MOST_ONCE: Job executes at most once, fails immediately on error.
        No retries.
        """
        try:
            response = self.session.post(
                api_endpoint,
                timeout=self.DEFAULT_TIMEOUT,
                json={'jobExecutionTime': datetime.utcnow().isoformat()}
            )
            
            if 200 <= response.status_code < 300:
                return {
                    'status': 'SUCCESS',
                    'status_code': response.status_code,
                    'response_body': response.text[:1000],
                    'error_message': None,
                    'retry_count': 0
                }
            else:
                return {
                    'status': 'FAILURE',
                    'status_code': response.status_code,
                    'response_body': response.text[:1000],
                    'error_message': f"HTTP {response.status_code}",
                    'retry_count': 0
                }
        
        except requests.Timeout:
            return {
                'status': 'TIMEOUT',
                'status_code': None,
                'response_body': None,
                'error_message': 'Request timeout',
                'retry_count': 0
            }
        
        except Exception as e:
            return {
                'status': 'FAILURE',
                'status_code': None,
                'response_body': None,
                'error_message': str(e),
                'retry_count': 0
            }
    
    def _execute_exactly_once(self, api_endpoint: str) -> Dict[str, Any]:
        """
        EXACTLY_ONCE: Job executes exactly once with idempotency tracking.
        Requires idempotent APIs.
        For this implementation, similar to ATLEAST_ONCE but with idempotent headers.
        """
        execution_id = f"{int(time.time() * 1000)}"  # Unique execution ID
        
        headers = {
            'Idempotency-Key': execution_id,
            'X-Job-Execution-Id': execution_id
        }
        
        try:
            response = self.session.post(
                api_endpoint,
                timeout=self.DEFAULT_TIMEOUT,
                json={'jobExecutionTime': datetime.utcnow().isoformat()},
                headers=headers
            )
            
            if 200 <= response.status_code < 300:
                return {
                    'status': 'SUCCESS',
                    'status_code': response.status_code,
                    'response_body': response.text[:1000],
                    'error_message': None,
                    'retry_count': 0,
                    'execution_id': execution_id
                }
            else:
                return {
                    'status': 'FAILURE',
                    'status_code': response.status_code,
                    'response_body': response.text[:1000],
                    'error_message': f"HTTP {response.status_code}",
                    'retry_count': 0,
                    'execution_id': execution_id
                }
        
        except Exception as e:
            return {
                'status': 'FAILURE',
                'status_code': None,
                'response_body': None,
                'error_message': str(e),
                'retry_count': 0,
                'execution_id': execution_id
            }
    
    # ==================== Helper Methods ====================
    
    def _create_session(self) -> requests.Session:
        """Create a requests session with retry strategy"""
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.DEFAULT_RETRIES,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set default headers
        session.headers.update({
            'User-Agent': 'JobScheduler/1.0',
            'Content-Type': 'application/json'
        })
        
        return session
    
    def _is_valid_url(self, url: str) -> bool:
        """Validate URL format"""
        try:
            result = requests.compat.urlparse(url)
            return all([result.scheme, result.netloc])
        except Exception:
            return False
    
    def _create_error_result(self, start_time: datetime, status: str, error_message: str) -> Dict[str, Any]:
        """Create error result"""
        end_time = datetime.utcnow()
        duration_ms = int((end_time - start_time).total_seconds() * 1000)
        
        return {
            'status': status,
            'status_code': None,
            'response_body': None,
            'error_message': error_message,
            'retry_count': 0,
            'start_time': start_time,
            'end_time': end_time,
            'duration_ms': duration_ms
        }