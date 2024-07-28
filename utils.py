from errors import RateLimitException
import time
import logging


def retry_on_rate_limit_exception(func, retries : int =  4, delay : float = 5.0):
    def wrapper(*args):
        for attempt in range(retries): 
            try:
                result = func(*args)
                return result
            except RateLimitException:
                logging.debug('rate limit handling')
                time.sleep(delay)
                continue
    return wrapper