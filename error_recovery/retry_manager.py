import time
import logging
import random
from functools import wraps

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RetryManager:
    """
    Manages retry attempts for operations that might fail temporarily
    """
    
    def __init__(self, max_retries=3, base_delay=1.0, max_delay=60.0, exponential_backoff=True):
        """
        Initialize the retry manager
        
        Args:
            max_retries: Maximum number of retry attempts
            base_delay: Initial delay between retries in seconds
            max_delay: Maximum delay between retries in seconds
            exponential_backoff: Whether to use exponential backoff
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_backoff = exponential_backoff
    
    def with_retry(self, func):
        """
        Decorator to wrap a function with retry logic
        
        Args:
            func: Function to wrap with retry logic
            
        Returns:
            Wrapped function that will be retried on failure
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(1, self.max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    # Calculate delay for next retry
                    if self.exponential_backoff:
                        delay = min(self.base_delay * (2 ** (attempt - 1)), self.max_delay)
                    else:
                        delay = self.base_delay
                    
                    # Add jitter to avoid thundering herd problem
                    delay = delay * (0.5 + random.random())
                    
                    logger.warning(
                        f"Attempt {attempt}/{self.max_retries} failed for {func.__name__}: {str(e)}. "
                        f"Retrying in {delay:.2f} seconds..."
                    )
                    
                    time.sleep(delay)
            
            # If we get here, all retries failed
            logger.error(f"All {self.max_retries} retry attempts failed for {func.__name__}")
            raise last_exception
            
        return wrapper
    
    def retry_operation(self, operation, *args, **kwargs):
        """
        Retry an operation with the configured retry policy
        
        Args:
            operation: Function to retry
            *args, **kwargs: Arguments to pass to the function
            
        Returns:
            Result of the operation if successful
            
        Raises:
            Exception: If all retry attempts fail
        """
        last_exception = None
        
        for attempt in range(1, self.max_retries + 1):
            try:
                return operation(*args, **kwargs)
            except Exception as e:
                last_exception = e
                
                # Calculate delay for next retry
                if self.exponential_backoff:
                    delay = min(self.base_delay * (2 ** (attempt - 1)), self.max_delay)
                else:
                    delay = self.base_delay
                
                # Add jitter to avoid thundering herd problem
                delay = delay * (0.5 + random.random())
                
                logger.warning(
                    f"Attempt {attempt}/{self.max_retries} failed: {str(e)}. "
                    f"Retrying in {delay:.2f} seconds..."
                )
                
                time.sleep(delay)
        
        # If we get here, all retries failed
        logger.error(f"All {self.max_retries} retry attempts failed")
        raise last_exception