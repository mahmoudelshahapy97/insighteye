from logging.config import dictConfig
import os
import logging
from logging.handlers import RotatingFileHandler

# def setup_logging(log_file_path: str = "/app/logs/app.log"):
#     os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

#     dictConfig({
#         "version": 1,
#         "disable_existing_loggers": False,

#         "formatters": {
#             "default": {
#                 "format": "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
#             }
#         },

#         "handlers": {
#             "console": {
#                 "class": "logging.StreamHandler",
#                 "formatter": "default",
#                 "level": "INFO"
#             },
#             "file": {
#                 "class": "logging.FileHandler",
#                 "filename": log_file_path,
#                 "formatter": "default",
#                 "level": "INFO",
#                 "mode": "a"
#             }
#         },

#         "loggers": {
#             "": {  # root logger
#                 "handlers": ["console", "file"],
#                 "level": "INFO",
#                 "propagate": False
#             },
#             "uvicorn": {
#                 "handlers": ["console", "file"],
#                 "level": "INFO",
#                 "propagate": False
#             },
#             "uvicorn.error": {
#                 "handlers": ["console", "file"],
#                 "level": "INFO",
#                 "propagate": False
#             },
#             "uvicorn.access": {
#                 "handlers": ["console", "file"],
#                 "level": "INFO",
#                 "propagate": False
#             },
#         }
#     })

def setup_logging(log_file_path: str = "/app/logs/app.log"):
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    file_handler = RotatingFileHandler(
        log_file_path,
        maxBytes=100 * 1024,   # 100 KB
        backupCount=3
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    root.addHandler(file_handler)
    root.addHandler(console_handler)

    # Ensure uvicorn uses same handlers
    for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(logger_name)
        logger.handlers.clear()
        logger.propagate = True
