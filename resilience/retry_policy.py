import logging
import os

from tenacity import (
    Retrying,
    before_sleep_log,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_exponential_jitter,
)

from resilience.exceptions import TransientDatabaseError

logger = logging.getLogger(__name__)


def get_retry_policy(with_jitter:bool=True) -> Retrying:
    max_attempts = int(os.getenv("RETRY_MAX_ATTEMPTS", "5"))
    if(with_jitter):
        return Retrying(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential_jitter(initial=1, max=60),
            retry=retry_if_exception_type(TransientDatabaseError),
            before_sleep=before_sleep_log(logger, logging.INFO),
            reraise=True,
        )
    else:
        return Retrying(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(multiplier=1, min=1, max=10),
            retry=retry_if_exception_type(TransientDatabaseError),
            before_sleep=before_sleep_log(logger, logging.INFO),
            reraise=True,
        )
