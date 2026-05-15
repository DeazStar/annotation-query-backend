import os
import logging
import axiom_py
from axiom_py.logging import AxiomHandler
import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration
from dotenv import load_dotenv

load_dotenv()

def init_logging():
    # --- Sentry ---
    DSN = os.getenv("SENTRY_DSN")
    if DSN:
        sentry_logging = LoggingIntegration(
            level=logging.INFO,        # Capture >= INFO as breadcrumbs
            event_level=logging.ERROR  # Send ERROR and above as events
        )

        sentry_sdk.init(
            dsn=DSN,
            integrations=[sentry_logging],
            send_default_pii=True,
            attach_stacktrace=True
        )

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Optional: also log to console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    root_logger.addHandler(console_handler)
    
    # --- Performance logger ---
    perf_logger = logging.getLogger("performance")
    perf_logger.setLevel(logging.INFO)
    perf_logger.addHandler(console_handler)

    # --- Axiom ---
    if os.getenv("AXIOM_TOKEN"):
        client = axiom_py.Client()
        dataset_name = os.getenv("AXIOM_DATASET", "application-logs")  # configurable
        axiom_handler = AxiomHandler(client, dataset_name)
        root_logger.addHandler(axiom_handler)
        
        PERF_LOGS_DATASET = os.getenv("AXIOM_PERFORMANCE_LOGS", "performance-metrics")
        perf_handler = AxiomHandler(client, PERF_LOGS_DATASET)
        perf_logger.addHandler(perf_handler)
    
    return perf_logger
