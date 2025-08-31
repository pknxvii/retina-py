import logging
import structlog
from fastapi import FastAPI
from app.config_loader import configuration
from app.api.routes import router

# Configure logging from YAML config
log_level = logging.ERROR
if configuration["log"]["level"] == "DEBUG":
    log_level = logging.DEBUG
elif configuration["log"]["level"] == "INFO":
    log_level = logging.INFO
elif configuration["log"]["level"] == "WARN":
    log_level = logging.WARN
elif configuration["log"]["level"] == "ERROR":
    log_level = logging.ERROR
elif configuration["log"]["level"] == "CRITICAL":
    log_level = logging.CRITICAL

# Configure structlog
structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S.%f", utc=True),
        structlog.processors.JSONRenderer()
    ],
    wrapper_class=structlog.make_filtering_bound_logger(log_level),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True
)

log = structlog.get_logger()

# Create FastAPI app with config
app = FastAPI(
    title=configuration["api"]["title"],
    debug=configuration["api"]["debug"]
)
app.include_router(router)